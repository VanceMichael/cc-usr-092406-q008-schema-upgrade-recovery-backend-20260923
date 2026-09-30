"""真正的多进程互斥测试：同一数据库同时启动多个进程。

覆盖：
* 同时启动 N 个进程，只有一个成为迁移者，其余等待后共同就绪；
* 迁移者在复制中途被硬杀（os._exit，锁不释放），等待者在心跳过期后接管，
  从断点完成迁移，不重复搬运。
"""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from db_fixtures import build_database, count_rows, make_engine, meta_value

from app.migrations import CURRENT_VERSION, engine as me

WORKER = Path(__file__).resolve().parent / "_mp_worker.py"
PYTHON = sys.executable


def spawn_worker(db_path: Path, extra_env=None) -> subprocess.Popen:
    env = dict(os.environ)
    env.update({
        "MIGRATION_COPY_BATCH_SIZE": "2",
        "MIGRATION_STEP_SLEEP": "0.35",
        "MIGRATION_LOCK_STALE_SECONDS": "2",
        "MIGRATION_POLL_INTERVAL_SECONDS": "0.1",
        "MIGRATION_WAIT_TIMEOUT_SECONDS": "1",
    })
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        [PYTHON, str(WORKER), str(db_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, text=True,
    )


def read_result(proc: subprocess.Popen, timeout=90):
    stdout, stderr = proc.communicate(timeout=timeout)
    if proc.returncode != 0 and not stdout.strip():
        raise AssertionError(f"worker 退出码 {proc.returncode}\n{stderr[-2000:]}")
    return json.loads(stdout.strip().splitlines()[-1])


class ConcurrentMigrationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_only_one_migrator_when_four_processes_start_together(self):
        db = self.tmp / "shared.db"
        build_database(db, 1, ponds=13, batches=3)

        procs = [spawn_worker(db) for _ in range(4)]
        results = [read_result(p) for p in procs]

        self.assertTrue(all(r["state"] == "ready" for r in results),
                        [r["state"] for r in results])
        self.assertEqual(sum(1 for r in results if r["migrator"]), 1,
                         "必须恰好有一个迁移者")
        # 等待者确实经历过 waiting，而不是各跑各的
        waiters = [r for r in results if not r["migrator"]]
        self.assertTrue(any("waiting" in r["states_seen"] for r in waiters),
                        [r["states_seen"] for r in waiters])

        # 数据完整、无重复搬运
        con = sqlite3.connect(str(db))
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 13)
            self.assertEqual(
                con.execute("SELECT COUNT(*) - COUNT(DISTINCT id) FROM ponds").fetchone()[0], 0
            )
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM schema_migration_lock").fetchone()[0], 0
            )
        finally:
            con.close()
        self.assertEqual(meta_value(db, "version"), str(CURRENT_VERSION))

    def test_waiter_takes_over_after_migrator_hard_crashes(self):
        db = self.tmp / "crasher.db"
        build_database(db, 1, ponds=8)

        # 迁移者：搬 2 批（4 行）后 os._exit 硬退出，锁不释放
        crasher = spawn_worker(db, {"MIGRATION_FAULT_AFTER_BATCHES": "2"})
        # 稍后启动两个等待者，确保迁移者已先持锁
        time.sleep(1.5)
        waiters = [spawn_worker(db), spawn_worker(db)]

        stdout, _ = crasher.communicate(timeout=30)
        self.assertEqual(crasher.returncode, 9, "故障进程应以 os._exit(9) 硬退出")
        self.assertEqual(stdout.strip(), "", "硬退出前不应输出任何结果")
        waiter_results = [read_result(w) for w in waiters]

        # 至少一个等待者接管完成；最终都能就绪
        self.assertTrue(all(r["state"] == "ready" for r in waiter_results),
                        [r["state"] for r in waiter_results])
        self.assertEqual(sum(1 for r in waiter_results if r["migrator"]), 1)

        con = sqlite3.connect(str(db))
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 8)
            codes = [r[0] for r in con.execute(
                "SELECT pond_code FROM ponds ORDER BY id")]
            self.assertEqual(codes, [f"P{i:06d}" for i in range(1, 9)])
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM schema_migration_lock").fetchone()[0], 0
            )
        finally:
            con.close()

    def test_second_start_after_retained_database_is_a_noop(self):
        """保留数据库文件的第二次部署：直接就绪，无人迁移。"""
        db = self.tmp / "retain.db"
        build_database(db, 1, ponds=6)
        first = read_result(spawn_worker(db))
        self.assertEqual(first["state"], "ready")
        self.assertTrue(first["migrator"])

        second = read_result(spawn_worker(db))
        self.assertEqual(second["state"], "ready")
        self.assertFalse(second["migrator"])
        self.assertEqual(count_rows(db, "ponds"), 6)
        self.assertEqual(meta_value(db, "version"), str(CURRENT_VERSION))


if __name__ == "__main__":
    unittest.main()
