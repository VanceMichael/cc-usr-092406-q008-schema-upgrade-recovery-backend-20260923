"""多进程测试用的迁移工作进程。

用法：python3 _mp_worker.py <数据库路径>

通过环境变量接收迁移调参与故障注入（见 app.migrations.engine 模块顶部）。
循环调用 startup_migrate，直到 ready/blocked/future_version，最后向 stdout
打印一行 JSON：

    {"state", "version", "migrator", "pid", "attempts", "states_seen"}
"""

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from sqlalchemy import create_engine

from app.migrations import engine as me


def main() -> None:
    db_path = sys.argv[1]
    eng = create_engine(f"sqlite:///{db_path}")

    states_seen = []
    migrator = False
    attempts = 0
    last = None
    while True:
        attempts += 1
        result = me.startup_migrate(eng, wait=1.0)
        states_seen.append(result.state)
        if result.migrator:
            migrator = True
        last = result
        if result.state in ("ready", "blocked", "future_version"):
            break
        if attempts > 60:  # 安全上限：最多等约 60s
            break
        time.sleep(0.2)

    print(json.dumps({
        "state": last.state,
        "version": last.version,
        "migrator": migrator,
        "pid": os.getpid(),
        "attempts": attempts,
        "states_seen": states_seen,
    }), flush=True)


if __name__ == "__main__":
    main()
