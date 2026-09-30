import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .routers import ponds, batches, stocking, feeding, water_quality, medication, costs, harvest, analysis
from .startup import health as health_report, readiness, run_startup

app = FastAPI(
    title="水产养殖管理系统",
    description="一个完整的水产养殖管理系统，支持塘口管理、投苗记录、日常管理、成本核算、出塘销售和养殖周期分析",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 结构未就绪时只放行这些路径（健康探针与根信息），其余业务请求返回 503。
_OPEN_PATHS = {"/health", "/", "/docs", "/openapi.json", "/redoc"}


@app.middleware("http")
async def require_ready_schema(request: Request, call_next):
    if request.url.path not in _OPEN_PATHS:
        info = readiness()
        if not info["ready"]:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "数据库结构尚未就绪，拒绝接流量",
                    "schema_status": info["schema_status"],
                    "version": info.get("version"),
                    "expected_version": info.get("expected_version"),
                    "reason": info.get("detail"),
                    "retry_after_seconds": 5,
                },
                headers={"Retry-After": "5"},
            )
    return await call_next(request)


@app.on_event("startup")
def _startup_migrations():
    run_startup()


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
        "version": "1.0.0"
    }

@app.get("/health")
def health_check():
    """分别反映进程存活、数据库可用、结构就绪。

    结构未就绪（迁移中/等待人工介入/未来版本）时返回 503，但进程与数据库
    两项检查仍独立可见，便于编排系统区分“正在启动”与“实例故障”。
    """
    report = health_report()
    status_code = 200 if report["status"] == "healthy" else 503
    return JSONResponse(status_code=status_code, content=report)
