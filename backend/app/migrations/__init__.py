"""可恢复的 SQLite 结构版本迁移框架。

对外主要入口：

* :func:`startup_migrate` —— 进程启动时执行/等待迁移；
* :func:`inspect_status` —— 读取当前迁移状态（供健康检查使用）；
* :class:`FutureVersionError` / :class:`MigrationBlocked` —— 禁止降级、需人工介入等拒绝态。
"""

from .engine import (
    CURRENT_VERSION,
    FutureVersionError,
    MigrationBlocked,
    MigrationCrash,
    MigrationWaitTimeout,
    StartupResult,
    inspect_status,
    set_faults,
    startup_migrate,
)

__all__ = [
    "CURRENT_VERSION",
    "FutureVersionError",
    "MigrationBlocked",
    "MigrationCrash",
    "MigrationWaitTimeout",
    "StartupResult",
    "inspect_status",
    "set_faults",
    "startup_migrate",
]
