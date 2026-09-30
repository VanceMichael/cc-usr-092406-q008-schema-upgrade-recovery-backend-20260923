"""数据库结构版本与可恢复迁移。

模块组成：

* :mod:`app.migrations.schema`   —— 结构指纹、校验摘要、不变量原语
* :mod:`app.migrations.versions` —— 每一个版本：前置版本、期望摘要、升级步骤、升级后不变量
* :mod:`app.migrations.engine`   —— 单迁移者协调、断点续传、回滚与人工介入判定
"""

from .engine import (
    CURRENT_VERSION,
    MigrationError,
    FutureVersionError,
    SchemaChecksumError,
    ManualInterventionRequired,
    MigrationInProgress,
    MigrationRolledBack,
    MigrationResult,
    run_migrations,
    wait_for_migrations,
    current_status,
    database_file_path,
)

__all__ = [
    "CURRENT_VERSION",
    "MigrationError",
    "FutureVersionError",
    "SchemaChecksumError",
    "ManualInterventionRequired",
    "MigrationInProgress",
    "MigrationRolledBack",
    "MigrationResult",
    "run_migrations",
    "wait_for_migrations",
    "current_status",
    "database_file_path",
]
