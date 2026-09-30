"""FastAPI 应用入口。

启动流程不再直接 ``create_all``：结构由 :mod:`app.migrations` 按版本管理。

* 迁移在后台线程执行（同库多进程通过 SQLite 单行锁互斥，只有一个迁移者）；
* 迁移完成前，进程存活可探活，但业务接口一律返回 503，清晰拒绝接流量；
* ``/health/live`` 只反映进程存活；``/health/ready``（以及 ``/health``）
  分别报告数据库可用性与结构就绪状态。
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .database import engine as default_engine, get_db
from .migrations import (
    FutureVersionError,
    MigrationBlocked,
    MigrationWaitTimeout,
    inspect_status,
    startup_migrate,
)
from .routers import ponds, batches, stocking, feeding, water_quality, medication, costs, harvest, analysis


def _make_get_db(db_engine):
    from sqlalchemy.orm import sessionmaker

    session_factory = sessionmaker(bind=db_engine, autocommit=False, autoflush=False)

    def _get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    return _get_db


class MigrationState:
    """进程内的迁移状态，后台线程写、请求线程读。"""
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.phase = "starting"       # starting | running | ready | blocked | future_version
        self.detail = "迁移尚未开始"
        self.version: Optional[int] = None
        self.migrator = False
        self.worker_started = False

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "phase": self.phase,
                "detail": self.detail,
                "version": self.version,
                "migrator": self.migrator,
            }

    def update(self, **kw) -> None:
        with self.lock:
            for key, value in kw.items():
                setattr(self, key, value)


def _migration_worker(app: FastAPI, first: threading.Event) -> None:
    state: MigrationState = app.state.migration
    db_engine = app.state.db_engine
    while True:
        try:
            result = startup_migrate(db_engine, wait=5)
        except (FutureVersionError, MigrationBlocked) as exc:
            state.update(phase="blocked", detail=str(exc))
            first.set()
            return
        except MigrationWaitTimeout as exc:
            state.update(phase="running", detail=f"等待唯一迁移者：{exc}")
            first.set()
            continue
        except Exception as exc:  # 数据库不可用等：持续重试，不接流量
            state.update(phase="blocked", detail=f"迁移异常：{exc}")
            first.set()
            continue

        if result.state == "ready":
            state.update(
                phase="ready",
                detail=result.detail,
                version=result.version,
                migrator=result.migrator,
            )
            first.set()
            return
        if result.state == "future_version":
            state.update(phase="future_version", detail=result.detail,
                        version=result.version)
            first.set()
            return
        if result.state == "blocked":
            state.update(phase="blocked", detail=result.detail)
            first.set()
            return
        # waiting：其他进程正在迁移，本进程保持未就绪并继续等待
        state.update(phase="running", detail=result.detail or "等待其他进程完成迁移")
        first.set()
        if result.state == "interrupted":
            # 本进程的迁移尝试在中途被打断（故障/连接异常）：稍后按断点重试
            time.sleep(0.5)


def create_app(db_engine=None) -> FastAPI:
    app = FastAPI(
        title="水产养殖管理系统",
        description="一个完整的水产养殖管理系统，支持塘口管理、投苗记录、日常管理、成本核算、出塘销售和养殖周期分析",
        version="3.0.0"
    )
    if db_engine is None:
        db_engine = default_engine
    app.state.db_engine = db_engine
    app.state.migration = MigrationState()

    # 路由通过依赖注入拿会话：测试或嵌入式部署可换绑到其他引擎/文件
    app.dependency_overrides[get_db] = _make_get_db(db_engine)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def gate_unready_requests(request: Request, call_next):
        """结构未就绪时，业务接口以明确的 503 拒绝流量。"""
        path = request.url.path
        if path.startswith("/api/") and app.state.migration.phase != "ready":
            snap = app.state.migration.snapshot()
            status_code = 503
            phase_map = {
                "starting": ("service_starting", "服务启动中，结构迁移尚未开始"),
                "running": ("schema_migrating", "数据库结构迁移中，暂不接收流量"),
                "blocked": ("migration_blocked", "数据库迁移需人工介入，暂不接收流量"),
                "future_version": ("unsupported_future_version",
                                   "数据库版本高于本程序，禁止降级启动"),
            }
            code, message = phase_map.get(snap["phase"], ("not_ready", "服务未就绪"))
            return JSONResponse(
                status_code=status_code,
                content={
                    "detail": message,
                    "code": code,
                    "migration": snap,
                },
                headers={"Retry-After": "2"},
            )
        return await call_next(request)

    app.include_router(ponds.router)
    app.include_router(batches.router)
    app.include_router(stocking.router)
    app.include_router(feeding.router)
    app.include_router(water_quality.router)
    app.include_router(medication.router)
    app.include_router(costs.router)
    app.include_router(harvest.router)
    app.include_router(analysis.router)

    @app.get("/")
    def root():
        return {
            "message": "欢迎使用水产养殖管理系统API",
            "docs": "/docs",
            "version": "3.0.0"
        }

    @app.get("/health/live")
    def health_live():
        """进程存活探针：只要事件循环在响应即为存活。"""
        return {"status": "alive", "process": "up"}

    @app.get("/health/ready")
    def health_ready():
        """就绪探针：进程存活 + 数据库可用 + 结构为当前版本，三者同时满足。"""
        snap = app.state.migration.snapshot()
        db_status = inspect_status(app.state.db_engine)
        alive = True
        database_up = db_status["database"] == "up"
        schema_state = db_status["schema"]
        schema_ready = schema_state == "ready"
        ready = alive and database_up and schema_ready and snap["phase"] == "ready"
        body = {
            "status": "ready" if ready else "not_ready",
            "checks": {
                "process": "up" if alive else "down",
                "database": db_status["database"],
                "schema": schema_state,
            },
            "schema_version": db_status["version"],
            "target_version": db_status["target"],
            "detail": db_status["detail"] or snap["detail"],
        }
        return JSONResponse(status_code=200 if ready else 503, content=body)

    @app.get("/health")
    def health():
        """综合健康：与 /health/ready 同语义（保留旧路径）。"""
        return health_ready()

    @app.on_event("startup")
    def _start_migration() -> None:
        state = app.state.migration
        first = threading.Event()
        thread = threading.Thread(
            target=_migration_worker, args=(app, first), name="db-migration",
            daemon=True,
        )
        state.worker_started = True
        state.phase = "running"
        thread.start()
        # 迁移很快（全新库/已就绪库）时，首拍同步等一下，避免无谓的 503 窗口
        first.wait(timeout=10)

    return app


app = create_app()
