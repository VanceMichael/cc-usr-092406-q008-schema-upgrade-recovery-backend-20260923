"""启动流程：结构迁移协调与接流量就绪判定。

进程角色
--------

* **迁移者**：拿到 ``<db>.migrate.lock`` 排他锁的进程，执行（或断点续传）迁移；
* **跟随者**：其余进程。默认 ``wait``：等待锁并在拿到后复核；``reject`` 模式下
  锁被占用即进入“迁移进行中”状态，业务请求一律 503，只放行健康检查。

启动失败不会让进程退出：未来版本、需要人工介入、结构漂移等情况下进程仍然存活，
但健康检查如实报告、业务接口以 503 拒绝接流量。

环境变量
~~~~~~~~

* ``MIGRATION_MODE``：``wait``（默认）或 ``reject``；
* ``MIGRATION_WAIT_TIMEOUT``：跟随者等待迁移者的秒数（默认 60）。
"""

from __future__ import annotations

import os
import threading
from typing import Optional

from .migrations import (
    CURRENT_VERSION,
    FutureVersionError,
    ManualInterventionRequired,
    MigrationError,
    MigrationInProgress,
    MigrationRolledBack,
    SchemaChecksumError,
    current_status,
    run_migrations,
)

MODE = os.getenv("MIGRATION_MODE", "wait").lower()
WAIT_TIMEOUT = float(os.getenv("MIGRATION_WAIT_TIMEOUT", "60"))

OUTCOME_READY = "ready"
OUTCOME_PENDING = "migration_in_progress"
OUTCOME_FAILED = "migration_failed"

_state_lock = threading.Lock()
_state = {
    "started": False,
    "outcome": None,       # ready / migration_in_progress / migration_failed
    "detail": "",
    "result": None,
}


def run_startup() -> dict:
    """执行一次启动迁移；重复调用直接返回已有时序结果（线程安全）。"""
    if _state["started"]:
        return _state
    with _state_lock:
        if _state["started"]:
            return _state
        try:
            result = run_migrations(mode=MODE, lock_timeout=WAIT_TIMEOUT)
            _state.update(outcome=OUTCOME_READY, detail="结构就绪",
                          result=result.as_dict())
        except MigrationInProgress as exc:
            _state.update(outcome=OUTCOME_PENDING, detail=str(exc), result=None)
        except (FutureVersionError, ManualInterventionRequired, SchemaChecksumError,
                MigrationRolledBack) as exc:
            _state.update(outcome=OUTCOME_FAILED, detail=str(exc), result=None)
        except MigrationError as exc:
            _state.update(outcome=OUTCOME_FAILED, detail=str(exc), result=None)
        _state["started"] = True
    return _state


def reset_for_tests() -> None:
    """清除启动时序缓存（仅供测试在切换目标数据库后调用）。"""
    with _state_lock:
        _state.update(started=False, outcome=None, detail="", result=None)


def readiness() -> dict:
    """当前是否可接业务流量。未就绪时实时复核数据库（跟随者据此自动转就绪）。"""
    st = run_startup()
    outcome = st["outcome"]

    if outcome == OUTCOME_READY:
        return {
            "ready": True,
            "schema_status": "ready",
            "version": CURRENT_VERSION,
            "expected_version": CURRENT_VERSION,
            "detail": "结构就绪",
            "startup": st["result"],
        }

    # 未就绪：读实时状态，迁移者可能已经在别的进程里完成
    live = current_status()
    if live["schema_ready"]:
        return {
            "ready": True,
            "schema_status": "ready",
            "version": live["version"],
            "expected_version": CURRENT_VERSION,
            "detail": "结构已由其他进程迁移完成",
            "startup": None,
        }

    schema_status = {
        OUTCOME_PENDING: "migration_in_progress",
        OUTCOME_FAILED: live.get("migration_state") or "migration_failed",
    }[outcome]
    return {
        "ready": False,
        "schema_status": schema_status,
        "version": live.get("version"),
        "expected_version": CURRENT_VERSION,
        "detail": live.get("detail") or st["detail"],
        "database_status": live.get("database"),
        "startup": None,
    }


def health() -> dict:
    """健康检查的三项独立信号：进程存活、数据库可用、结构就绪。"""
    process_alive = True
    st = run_startup()
    live = current_status()

    database_ok = live["database"] == "available"
    schema_ready = bool(live["schema_ready"])

    checks = {
        "process": {
            "status": "alive" if process_alive else "dead",
            "detail": "进程存活",
        },
        "database": {
            "status": live["database"],
            "detail": "数据库可连接" if database_ok else live.get("detail", "数据库不可用"),
        },
        "schema": {
            "status": (
                "ready" if schema_ready
                else (live.get("migration_state") or st["outcome"] or "unknown")
            ),
            "version": live.get("version"),
            "expected_version": CURRENT_VERSION,
            "detail": live.get("detail") or st.get("detail", ""),
        },
    }

    if process_alive and database_ok and schema_ready:
        overall = "healthy"
    elif process_alive and database_ok:
        overall = "starting"          # 进程活着、库可连，但结构未就绪/迁移中
    else:
        overall = "unavailable"

    return {"status": overall, "checks": checks}
