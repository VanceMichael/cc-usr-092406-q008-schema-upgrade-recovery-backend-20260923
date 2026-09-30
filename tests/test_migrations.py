"""结构迁移引擎的自动化测试。

覆盖需求：
* 全新库直接建到当前版本；
* 沿用旧 SQLite 文件，跨至少两个旧版本升级（v1→v2→v3），并校验数据与不变量；
* 从 v2 旧版本升级（前置版本链）；
* 故障注入：搬运中断续跑（不重复搬运）、切换后中断续跑、校验失败自动回退、
  备份损坏转人工介入、人工状态跨重启保持；
* 同一数据库只有一个迁移者（真实多进程），跟随者等待或以清晰状态拒绝；
* 未知未来版本禁止降级启动；
* 保留数据库文件重启：幂等且数据仍在；
* 健康接口分别反映进程、数据库、结构，结构未就绪时业务流量被 503 拒绝。
"""

import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
sys.path.insert(0, str(BACKEND))

from app.migrations import (  # noqa: E402
    CURRENT_VERSION,
    FutureVersionError,
    ManualInterventionRequired,
    MigrationError,
    MigrationInProgress,
    MigrationRolledBack,
    current_status,
    run_migrations,
)
from app.migrations.engine import (  # noqa: E402
    COPY_BATCH_SIZE,
    _index_ddl,
    create_table_ddl,
)
from app.migrations.versions import SPEC_V1, SPEC_V2, VERSIONS  # noqa: E402

WORKDIR = Path(os.environ.get("MIGRATION_TEST_DIR", "/tmp/aqua_migration_tests"))


def _create_schema(conn, spec):
    for table in sorted(spec.tables, key=lambda t: t.name):
        conn.execute(create_table_ddl(table))
    for table in sorted(spec.tables, key=lambda t: t.name):
        for index in sorted(table.indexes, key=lambda i: i.name):
            conn.execute(_index_ddl(index, table.name))


def _seed_v1_data(conn):
    """写入覆盖全部业务表的样例数据，用于升级前后比对。"""
    conn.executescript(
        """
        INSERT INTO ponds(name, area, water_depth, species, status, created_at, updated_at)
        VALUES ('一号塘', 5.5, 2.0, '鲈鱼', 'active', '2026-01-01', '2026-01-01');
        INSERT INTO ponds(name, area, water_depth, species, status, created_at, updated_at)
        VALUES ('二号塘', 3.0, 1.8, '草鱼', 'active', '2026-01-02', '2026-01-02');
        INSERT INTO batches(batch_number, pond_id, species, stocking_date, status, created_at, updated_at)
        VALUES ('B2026001', 1, '鲈鱼', '2026-01-05', 'active', '2026-01-05', '2026-01-05');
        INSERT INTO stocking_records(batch_id, species, quantity, source, created_at)
        VALUES (1, '鲈鱼', 1000, '育苗场A', '2026-01-05');
        INSERT INTO feeding_records(batch_id, feeding_date, feed_type, feed_quantity, created_at)
        VALUES (1, '2026-02-01', '浮性颗粒料', 20.0, '2026-02-01');
        INSERT INTO feeding_records(batch_id, feeding_date, feed_type, feed_quantity, created_at)
        VALUES (1, '2026-02-02', '浮性颗粒料', 21.5, '2026-02-02');
        INSERT INTO water_quality_records(batch_id, record_date, ph_value, dissolved_oxygen, created_at)
        VALUES (1, '2026-02-01', 7.8, 6.5, '2026-02-01');
        INSERT INTO medication_records(batch_id, medication_date, drug_name, dosage, created_at)
        VALUES (1, '2026-02-10', '聚维酮碘', 0.5, '2026-02-10');
        INSERT INTO cost_records(batch_id, cost_date, cost_type, amount, created_at)
        VALUES (1, '2026-02-01', 'feed', 100.0, '2026-02-01');
        INSERT INTO cost_records(batch_id, cost_date, cost_type, amount, created_at)
        VALUES (1, '2026-02-05', 'electricity', 30.25, '2026-02-05');
        INSERT INTO harvest_sales(batch_id, sale_date, weight, unit_price, created_at)
        VALUES (1, '2026-06-01', 400.0, 22.0, '2026-06-01');
        """
    )


def build_legacy_v1(path: Path, seed=True) -> Path:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    try:
        _create_schema(conn, SPEC_V1)
        if seed:
            _seed_v1_data(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def build_db_at_version(path: Path, version: int) -> Path:
    """构造“已登记到指定版本”的数据库（含记账表），用于从中间版本起跑。"""
    if path.exists():
        path.unlink()
    spec = VERSIONS[version].schema
    conn = sqlite3.connect(path)
    try:
        _create_schema(conn, spec)
        conn.execute(
            "CREATE TABLE _schema_migrations (version INTEGER PRIMARY KEY, "
            "checksum TEXT NOT NULL, applied_at TEXT NOT NULL, description TEXT NOT NULL DEFAULT '')"
        )
        conn.execute(
            "CREATE TABLE _migration_state (id INTEGER PRIMARY KEY CHECK (id=1), "
            "from_version INTEGER, to_version INTEGER, step_index INTEGER, phase TEXT, "
            "last_copied_id INTEGER, rows_copied INTEGER, status TEXT, backup_path TEXT, "
            "message TEXT, updated_at TEXT)"
        )
        conn.execute(
            "CREATE TABLE _migration_row_counts (from_version INTEGER, table_name TEXT, "
            "row_count INTEGER, recorded_at TEXT, PRIMARY KEY(from_version, table_name))"
        )
        for v in range(1, version + 1):
            conn.execute(
                "INSERT INTO _schema_migrations(version, checksum, applied_at, description) "
                "VALUES (?,?,?,?)", (v, VERSIONS[v].schema_digest(), "2026-09-01", "fixture")
            )
        conn.execute(f"PRAGMA user_version={version}")
        conn.commit()
    finally:
        conn.close()
    return path


class VersionDefinitionTest(unittest.TestCase):
    def test_predecessors_form_a_strict_chain(self):
        for v in range(2, CURRENT_VERSION + 1):
            self.assertEqual(VERSIONS[v].predecessor, v - 1,
                             f"v{v} 必须以前一版为唯一前置版本")

    def test_each_version_has_stable_checksum(self):
        for v, mv in VERSIONS.items():
            digest = mv.schema_digest()
            self.assertRegex(digest, r"^[0-9a-f]{64}$")
            self.assertEqual(digest, mv.checksum)

    def test_highest_version_defines_invariants(self):
        # 至少声明结构性不变量，防止“空升级”
        self.assertTrue(VERSIONS[2].invariants)
        self.assertTrue(VERSIONS[3].invariants)


class FreshDatabaseTest(unittest.TestCase):
    def setUp(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        self.path = WORKDIR / "fresh.db"
        for suffix in ("", "-wal", "-shm", ".migrate.lock", ".pre-v2.bak", ".pre-v3.bak",
                       ".pre-v2.bak.sha256", ".pre-v3.bak.sha256", ".rolled-back"):
            p = Path(str(self.path) + suffix)
            if p.exists():
                p.unlink()

    def test_fresh_database_created_at_current_version(self):
        result = run_migrations(str(self.path))
        self.assertEqual(result.state, "created")
        self.assertEqual(result.to_version, CURRENT_VERSION)

        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], CURRENT_VERSION)
            applied = [r[0] for r in conn.execute(
                "SELECT version FROM _schema_migrations ORDER BY version")]
            self.assertEqual(applied, list(range(1, CURRENT_VERSION + 1)))
        finally:
            conn.close()

        status = current_status(f"sqlite:///{self.path}")
        self.assertTrue(status["schema_ready"])
        self.assertEqual(status["migration_state"], "up_to_date")

    def test_running_again_is_idempotent(self):
        run_migrations(str(self.path))
        result = run_migrations(str(self.path))
        self.assertEqual(result.state, "up_to_date")


class LegacyUpgradeTest(unittest.TestCase):
    def setUp(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        self.path = WORKDIR / "legacy_v1.db"
        build_legacy_v1(self.path)

    def test_upgrade_across_v1_v2_v3_preserves_data(self):
        result = run_migrations(str(self.path))
        self.assertEqual(result.from_version, 1)
        self.assertEqual(result.to_version, 3)
        self.assertEqual(result.state, "migrated")

        conn = sqlite3.connect(self.path)
        try:
            # 版本登记与结构
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
            applied = [r[0] for r in conn.execute(
                "SELECT version FROM _schema_migrations ORDER BY version")]
            self.assertEqual(applied, [1, 2, 3])

            pond_cols = {r[1]: (r[2], r[3]) for r in conn.execute("PRAGMA table_info(ponds)")}
            self.assertEqual(pond_cols["name"], ("VARCHAR(50)", 1))
            self.assertEqual(pond_cols["location_code"], ("VARCHAR(20)", 1))
            feed_cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(feeding_records)")}
            self.assertEqual(feed_cols["feed_quantity"].replace(" ", ""), "NUMERIC(10,2)")
            self.assertIn("feed_batch_number", feed_cols)
            cost_cols = {r[1]: (r[2], r[3]) for r in conn.execute("PRAGMA table_info(cost_records)")}
            self.assertEqual(cost_cols["cost_type"], ("VARCHAR(30)", 1))

            # v2 回填不变量：P + 8 位，唯一
            codes = [r[0] for r in conn.execute("SELECT location_code FROM ponds ORDER BY id")]
            self.assertEqual(codes, ["P00000001", "P00000002"])

            # 数据不丢、主键不乱
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM feeding_records").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM harvest_sales").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM medication_records").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM water_quality_records").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM stocking_records").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM cost_records").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*)-COUNT(DISTINCT id) FROM ponds").fetchone()[0], 0)

            # 数值在 FLOAT→NUMERIC 收紧后保持
            quantities = [r[0] for r in conn.execute(
                "SELECT feed_quantity FROM feeding_records ORDER BY id")]
            self.assertEqual(quantities, [20.0, 21.5])
            self.assertEqual(conn.execute(
                "SELECT name FROM ponds WHERE id=1").fetchone()[0], "一号塘")

            # 无残留影子表、无遗留备份、状态表清空
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '\\_\\_migrate%' ESCAPE '\\'"
            ).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM _migration_state").fetchone()[0], 0)
        finally:
            conn.close()

        self.assertFalse(Path(str(self.path) + ".pre-v3.bak").exists(),
                         "升级成功后应清理物理备份")

    def test_start_from_v2_runs_only_v3(self):
        path = WORKDIR / "from_v2.db"
        build_db_at_version(path, 2)
        result = run_migrations(str(path))
        self.assertEqual(result.from_version, 2)
        self.assertEqual(result.state, "migrated")
        conn = sqlite3.connect(path)
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
        finally:
            conn.close()

    def test_restart_with_database_preserved_is_stable(self):
        # 首次升级
        run_migrations(str(self.path))
        # 模拟进程重启：再次运行迁移（文件原样保留）
        second = run_migrations(str(self.path))
        self.assertEqual(second.state, "up_to_date")
        # 数据仍在
        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT location_code FROM ponds WHERE id=1").fetchone()[0],
                             "P00000001")
        finally:
            conn.close()

    def test_legacy_schema_not_matching_v1_is_rejected(self):
        # 无版本记账但结构不是已知 v1：拒绝猜测、拒绝迁移
        path = WORKDIR / "unknown_legacy.db"
        if path.exists():
            path.unlink()
        conn = sqlite3.connect(path)
        try:
            conn.execute("CREATE TABLE ponds (id INTEGER PRIMARY KEY, weird_col TEXT)")
            conn.execute("INSERT INTO ponds(weird_col) VALUES ('x')")
            conn.commit()
        finally:
            conn.close()
        from app.migrations import SchemaChecksumError
        with self.assertRaises(SchemaChecksumError):
            run_migrations(str(path))


class FailureInjectionTest(unittest.TestCase):
    def setUp(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        import app.migrations.engine as engine_mod
        self._orig_batch = engine_mod.COPY_BATCH_SIZE
        engine_mod.COPY_BATCH_SIZE = 500
        self.engine_mod = engine_mod

    def tearDown(self):
        self.engine_mod.COPY_BATCH_SIZE = self._orig_batch

    def _large_v1(self, name, n=1200):
        path = WORKDIR / name
        build_legacy_v1(path, seed=False)
        conn = sqlite3.connect(path)
        try:
            conn.executemany(
                "INSERT INTO ponds(name, area, water_depth) VALUES (?,?,?)",
                [(f"塘{i:05d}", float(i), 2.0) for i in range(1, n + 1)],
            )
            conn.commit()
        finally:
            conn.close()
        return path

    def test_interrupt_during_copy_resumes_without_duplicating(self):
        path = self._large_v1("crash_copy.db")
        calls = {"n": 0}

        def inject(step, phase, ctx):
            if step == "rebuild_ponds_v3" and phase == "copy":
                calls["n"] += 1
                if calls["n"] == 2:  # 第二批搬运开始时“进程被杀”
                    raise RuntimeError("simulated process kill mid-copy")

        with self.assertRaises(MigrationError):
            run_migrations(str(path), failure_injection=inject)

        conn = sqlite3.connect(path)
        try:
            phase, last_id, copied, status = conn.execute(
                "SELECT phase, last_copied_id, rows_copied, status FROM _migration_state"
            ).fetchone()
            self.assertEqual((phase, last_id, copied, status), ("copy", 500, 500, "running"))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM __migrate_ponds").fetchone()[0], 500)
        finally:
            conn.close()

        # 重启：从 id=500 之后继续，不重复搬运
        result = run_migrations(str(path))
        self.assertEqual(result.state, "continued")
        self.assertEqual(result.from_version, 2)
        conn = sqlite3.connect(path)
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 1200)
            self.assertEqual(conn.execute("SELECT COUNT(*)-COUNT(DISTINCT id) FROM ponds").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT location_code FROM ponds WHERE id=1200").fetchone()[0], "P00001200")
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '\\_\\_migrate%' ESCAPE '\\'"
            ).fetchone()[0], 0)
        finally:
            conn.close()

    def test_interrupt_after_switch_commit_does_not_recopy(self):
        path = self._large_v1("crash_switch.db", n=10)

        def inject(step, phase, ctx):
            if step == "rebuild_ponds_v3" and phase == "switched":
                raise RuntimeError("simulated kill right after switch commit")

        with self.assertRaises(MigrationError):
            run_migrations(str(path), failure_injection=inject)

        # 主表已是新结构、影子表已被改名且记账尚未推进
        conn = sqlite3.connect(path)
        try:
            name_type = [r[2] for r in conn.execute("PRAGMA table_info(ponds)") if r[1] == "name"][0]
            self.assertEqual(name_type, "VARCHAR(50)")
        finally:
            conn.close()

        result = run_migrations(str(path))
        self.assertEqual(result.state, "continued")
        conn = sqlite3.connect(path)
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 10)
        finally:
            conn.close()

    def test_verify_failure_with_good_backup_rolls_back(self):
        path = self._large_v1("rollback.db", n=10)

        def inject(step, phase, ctx):
            if step == "rebuild_ponds_v3" and phase == "verify":
                side = sqlite3.connect(path)
                try:
                    side.execute("UPDATE __migrate_ponds SET name='被篡改' WHERE id=1")
                    side.commit()
                finally:
                    side.close()

        with self.assertRaises(MigrationRolledBack):
            run_migrations(str(path), failure_injection=inject)

        # 数据库被物理回退到 v2（v3 的前置版本）
        conn = sqlite3.connect(path)
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 2)
            name_type = [r[2] for r in conn.execute("PRAGMA table_info(ponds)") if r[1] == "name"][0]
            self.assertEqual(name_type, "VARCHAR(100)")
            self.assertEqual(conn.execute("SELECT name FROM ponds WHERE id=1").fetchone()[0],
                             "塘00001")
        finally:
            conn.close()
        self.assertTrue(Path(str(path) + ".rolled-back").exists())

        # 留有回退标记时拒绝再次启动
        with self.assertRaises(ManualInterventionRequired):
            run_migrations(str(path))

        # 人工确认删除标记后，可在干净的 v2 上重新升级
        Path(str(path) + ".rolled-back").unlink()
        result = run_migrations(str(path))
        self.assertEqual(result.to_version, 3)

    def test_verify_failure_with_bad_backup_requires_manual(self):
        path = self._large_v1("manual.db", n=10)

        def inject(step, phase, ctx):
            if step == "rebuild_ponds_v3" and phase == "verify":
                side = sqlite3.connect(path)
                try:
                    side.execute("UPDATE __migrate_ponds SET name='被篡改' WHERE id=1")
                    side.commit()
                finally:
                    side.close()
                # 同时破坏回退能力
                for entry in WORKDIR.iterdir():
                    if entry.name.startswith("manual.db.pre-v3"):
                        entry.unlink()

        with self.assertRaises(ManualInterventionRequired):
            run_migrations(str(path), failure_injection=inject)

        # 跨重启仍然被冻结，直到人工处理
        for _ in range(2):
            with self.assertRaises(ManualInterventionRequired):
                run_migrations(str(path))
        status = current_status(f"sqlite:///{path}")
        self.assertEqual(status["migration_state"], "needs_manual")
        self.assertFalse(status["schema_ready"])


class FutureVersionTest(unittest.TestCase):
    def test_newer_database_refuses_downgrade_start(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        path = WORKDIR / "future.db"
        build_db_at_version(path, CURRENT_VERSION)
        conn = sqlite3.connect(path)
        try:
            conn.execute(f"PRAGMA user_version={CURRENT_VERSION + 5}")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaises(FutureVersionError):
            run_migrations(str(path))
        status = current_status(f"sqlite:///{path}")
        self.assertFalse(status["schema_ready"])
        self.assertEqual(status["migration_state"], "future_version")


class ConcurrencyTest(unittest.TestCase):
    """真实双进程：fcntl 锁是进程级的，线程无法验证互斥。"""

    def setUp(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        self.path = WORKDIR / "concurrent.db"
        build_legacy_v1(self.path, seed=False)
        conn = sqlite3.connect(self.path)
        try:
            conn.executemany(
                "INSERT INTO ponds(name, area, water_depth) VALUES (?,?,?)",
                [(f"塘{i:05d}", float(i), 2.0) for i in range(1, 3001)],
            )
            conn.commit()
        finally:
            conn.close()

    def _run_in_subprocess(self, body: str, env_extra=None):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(BACKEND) + os.pathsep + env.get("PYTHONPATH", "")
        env["MIGRATION_COPY_BATCH_SIZE"] = "100"
        if env_extra:
            env.update(env_extra)
        return subprocess.run(
            [sys.executable, "-c", textwrap.dedent(body)],
            capture_output=True, text=True, env=env, timeout=90,
        )

    def test_single_migrator_follower_rejects_then_waits(self):
        migrator = textwrap.dedent(f"""
            import sys, time
            sys.path.insert(0, {str(BACKEND)!r})
            from app.migrations import run_migrations
            def slow(step, phase, ctx):
                if phase == "copy":
                    time.sleep(0.02)
            r = run_migrations({str(self.path)!r}, failure_injection=slow)
            print("MIGRATOR", r.state, r.to_version)
        """)
        follower = textwrap.dedent(f"""
            import sys
            sys.path.insert(0, {str(BACKEND)!r})
            from app.migrations import run_migrations
            from app.migrations.engine import MigrationInProgress
            try:
                run_migrations({str(self.path)!r}, mode="reject")
                print("REJECT_ACQUIRED_BAD")
            except MigrationInProgress:
                print("REJECT_OK")
            r = run_migrations({str(self.path)!r}, mode="wait", lock_timeout=60)
            print("WAIT", r.state, r.to_version)
        """)

        proc = subprocess.Popen(
            [sys.executable, "-c", textwrap.dedent(migrator)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env={**os.environ, "PYTHONPATH": str(BACKEND), "MIGRATION_COPY_BATCH_SIZE": "100"},
        )
        time.sleep(0.8)
        follower_result = self._run_in_subprocess(follower)
        self.assertIn("REJECT_OK", follower_result.stdout)
        self.assertIn("WAIT up_to_date 3", follower_result.stdout)

        out, err = proc.communicate(timeout=90)
        self.assertEqual(proc.returncode, 0, err)
        self.assertIn("MIGRATOR migrated 3", out)

        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 3000)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '\\_\\_migrate%' ESCAPE '\\'"
            ).fetchone()[0], 0)
        finally:
            conn.close()


class HealthAndTrafficTest(unittest.TestCase):
    """健康接口三信号 + 结构未就绪时的 503 流量闸门（子进程隔离应用全局状态）。"""

    def test_health_reflects_three_signals_and_gates_traffic(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        db = WORKDIR / "notready.db"
        build_db_at_version(db, CURRENT_VERSION)
        # 制造一个 needs_manual 状态：直接写记账表
        conn = sqlite3.connect(db)
        try:
            conn.execute(
                "INSERT INTO _migration_state(id, from_version, to_version, step_index, "
                "phase, status, message, updated_at) VALUES (1,2,3,0,'verify',"
                "'needs_manual','测试：等待人工介入','2026-09-30')"
            )
            conn.commit()
        finally:
            conn.close()

        script = textwrap.dedent(f"""
            import os, sys, json
            os.environ["DATABASE_URL"] = "sqlite:///{db}"
            sys.path.insert(0, {str(BACKEND)!r})
            from fastapi.testclient import TestClient
            from app.main import app
            with TestClient(app) as client:
                h = client.get("/health")
                print("HEALTH", h.status_code)
                print(json.dumps(h.json()["checks"], ensure_ascii=False, sort_keys=True))
                root = client.get("/")
                print("ROOT", root.status_code)
                api = client.get("/api/ponds/")
                print("API", api.status_code, api.json().get("schema_status"))
        """)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(BACKEND) + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run([sys.executable, "-c", script],
                              capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        lines = proc.stdout.strip().splitlines()
        self.assertIn("HEALTH 503", lines)
        payload = json.loads(lines[1])
        self.assertEqual(payload["process"]["status"], "alive")
        self.assertEqual(payload["database"]["status"], "available")
        self.assertEqual(payload["schema"]["status"], "needs_manual")
        self.assertIn("ROOT 200", lines)
        self.assertIn("API 503 needs_manual", lines)

    def test_health_healthy_and_api_served_after_migration(self):
        WORKDIR.mkdir(parents=True, exist_ok=True)
        db = WORKDIR / "ready_http.db"
        build_legacy_v1(db)

        script = textwrap.dedent(f"""
            import os, sys
            os.environ["DATABASE_URL"] = "sqlite:///{db}"
            sys.path.insert(0, {str(BACKEND)!r})
            from fastapi.testclient import TestClient
            from app.main import app
            with TestClient(app) as client:
                h = client.get("/health")
                print("HEALTH", h.status_code, h.json()["status"])
                api = client.get("/api/ponds/")
                print("API", api.status_code, len(api.json()))
                create = client.post("/api/ponds/", json={{
                    "name": "新塘口", "area": 1.0, "water_depth": 1.0, "species": "鲫鱼"}})
                print("CREATE", create.status_code, create.json().get("location_code"))
        """)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(BACKEND) + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run([sys.executable, "-c", script],
                              capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = proc.stdout
        self.assertIn("HEALTH 200 healthy", out)
        self.assertIn("API 200 2", out)
        self.assertIn("CREATE 200 P00000003", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
