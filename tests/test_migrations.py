"""迁移引擎测试：全新库、跨版本升级、故障注入恢复、回退/人工、未来版本、重启。"""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from db_fixtures import BACKEND, build_database, count_rows, make_engine, meta_value

from app.migrations import CURRENT_VERSION, engine as me
from app.migrations import schema


class MigrationTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        # 每批很小，确保多批次搬运
        os.environ["MIGRATION_COPY_BATCH_SIZE"] = "3"
        me.COPY_BATCH_SIZE = 3
        me.set_faults({})
        self._copied_events = []
        me.on_copy_batch = self._record_copy

    def tearDown(self):
        me.set_faults({})
        me.on_copy_batch = lambda **kw: None
        self._tmp.cleanup()

    def _record_copy(self, moved, last_id, total, batch_index):
        self._copied_events.append((moved, last_id, total))

    def migrate(self, path):
        return me.startup_migrate(make_engine(path))

    def assert_ready_v3(self, path, ponds):
        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assertEqual(result.version, CURRENT_VERSION)
        self.assertEqual(meta_value(path, "version"), str(CURRENT_VERSION))
        self.assertEqual(count_rows(path, "ponds"), ponds)
        # 无残留临时表、无锁
        con = sqlite3.connect(str(path))
        try:
            leftovers = [
                r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE name IN "
                    "('ponds__new','ponds__old')"
                )
            ]
            self.assertEqual(leftovers, [])
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM schema_migration_lock").fetchone()[0], 0
            )
        finally:
            con.close()


class FreshDatabaseTests(MigrationTestBase):
    def test_fresh_database_initializes_at_current_version(self):
        path = self.tmp / "fresh.db"
        result = self.migrate(path)
        self.assertEqual(result.state, "ready")
        self.assertTrue(result.migrator)
        self.assertEqual(meta_value(path, "version"), str(CURRENT_VERSION))

        # 结构摘要与 ORM 建表产物一致：直接用 ORM 会话读写
        from sqlalchemy.orm import Session
        from app.models import Pond
        with Session(make_engine(path)) as db:
            db.add(Pond(name="新塘", area=5, water_depth=1.5, species="草鱼",
                        pond_code="P000009"))
            db.commit()
        self.assertEqual(count_rows(path, "ponds"), 1)

        # 再次启动幂等
        again = self.migrate(path)
        self.assertEqual(again.state, "ready")
        self.assertFalse(again.migrator)


class CrossVersionUpgradeTests(MigrationTestBase):
    def test_upgrade_from_v1_spans_two_versions(self):
        path = self.tmp / "v1.db"
        build_database(path, 1, ponds=10, batches=4)

        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assertTrue(result.migrator)

        con = sqlite3.connect(str(path))
        try:
            pond_cols = {r[1] for r in con.execute("PRAGMA table_info(ponds)")}
            batch_cols = {r[1] for r in con.execute("PRAGMA table_info(batches)")}
            self.assertIn("location_code", pond_cols)
            self.assertIn("pond_code", pond_cols)
            self.assertIn("notes", batch_cols)
            # v1 数据全部保留
            self.assertEqual(con.execute("SELECT COUNT(*) FROM ponds").fetchone()[0], 10)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM batches").fetchone()[0], 4)
            # NOT NULL 填充：历史 NULL species 被补默认值
            null_species = con.execute(
                "SELECT COUNT(*) FROM ponds WHERE species IS NULL OR species=''"
            ).fetchone()[0]
            self.assertEqual(null_species, 0)
            # pond_code 唯一且按 id 补码
            codes = [r[0] for r in con.execute("SELECT pond_code FROM ponds ORDER BY id")]
            self.assertEqual(codes, [f"P{i:06d}" for i in range(1, 11)])
            self.assertEqual(
                con.execute("SELECT COUNT(*) - COUNT(DISTINCT pond_code) FROM ponds")
                .fetchone()[0], 0
            )
            # 外键仍指向 ponds
            fk = [r[2] for r in con.execute("PRAGMA foreign_key_list(batches)")]
            self.assertIn("ponds", fk)
        finally:
            con.close()

        # 备份文件逐版生成
        self.assertTrue((self.tmp / "v1.db.v1.bak").exists())
        self.assertTrue((self.tmp / "v1.db.v2.bak").exists())

        status = me.inspect_status(make_engine(path))
        self.assertEqual(status["schema"], "ready")
        self.assertEqual(status["database"], "up")

    def test_upgrade_from_v2_copies_table_with_batching(self):
        path = self.tmp / "v2.db"
        build_database(path, 2, ponds=7)
        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        # 每批 3 行 -> 至少 3 个搬运批次事件（3+3+1+0）
        moved_batches = [e for e in self._copied_events if e[0] > 0]
        self.assertGreaterEqual(len(moved_batches), 2)
        self.assertEqual(sum(e[0] for e in moved_batches), 7)


class FaultInjectionRecoveryTests(MigrationTestBase):
    def test_crash_mid_copy_resumes_without_duplicate_transfer(self):
        path = self.tmp / "crash.db"
        build_database(path, 1, ponds=10)

        # 第一次：搬 2 批（6 行）后崩溃
        me.set_faults({"after_batches": 2})
        result = self.migrate(path)
        self.assertEqual(result.state, "interrupted")
        self.assertEqual(meta_value(path, "phase"), "copy")
        self.assertEqual(int(meta_value(path, "last_copied_id")), 6)
        self.assertEqual(int(meta_value(path, "rows_copied")), 6)
        self.assertEqual(count_rows(path, "ponds"), 10)  # 源表未被触碰
        self.assertEqual(count_rows(path, "ponds__new"), 6)

        # 第二次：从断点继续。记录本次实际搬运量，必须只搬剩余 4 行
        self._copied_events.clear()
        me.set_faults({})
        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        resumed_moved = sum(e[0] for e in self._copied_events if e[0] > 0)
        self.assertEqual(resumed_moved, 4, "恢复后重复搬运了已存在的行")
        self.assert_ready_v3(path, 10)

        con = sqlite3.connect(str(path))
        try:
            rows = con.execute(
                "SELECT id, pond_code, species FROM ponds ORDER BY id"
            ).fetchall()
            self.assertEqual([r[1] for r in rows], [f"P{i:06d}" for i in range(1, 11)])
            # 偶数 id 来自 v1 的 NULL species
            self.assertEqual(rows[1][2], "未指定品种")
        finally:
            con.close()

    def test_two_consecutive_crashes_each_continue_from_checkpoint(self):
        path = self.tmp / "crash2.db"
        build_database(path, 1, ponds=10)

        me.set_faults({"after_batches": 1})
        self.assertEqual(self.migrate(path).state, "interrupted")
        me.set_faults({"after_batches": 2})  # 相对本次续搬：再搬 2 批
        self.assertEqual(self.migrate(path).state, "interrupted")
        self.assertEqual(int(meta_value(path, "rows_copied")), 9)
        self._copied_events.clear()
        me.set_faults({})
        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assertEqual(sum(e[0] for e in self._copied_events if e[0] > 0), 1)
        self.assert_ready_v3(path, 10)

    def test_corrupt_shadow_triggers_rollback_then_restart(self):
        path = self.tmp / "rollback.db"
        build_database(path, 2, ponds=9)
        me.set_faults({"after_batches": 2})
        self.migrate(path)

        # 外力删掉影子表若干行：断点声称 6 行，实际少了
        con = sqlite3.connect(str(path))
        con.execute("DELETE FROM ponds__new WHERE id <= 2")
        con.commit()
        con.close()

        me.set_faults({})
        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assert_ready_v3(path, 9)
        # 回退尝试有记录
        self.assertIsNotNone(meta_value(path, "attempts"))
        # 源表行数始终没变
        self.assertEqual(
            count_rows(Path(str(path) + ".v2.bak"), "ponds"), 9
        )

    def test_crash_after_swap_finalizes_on_restart(self):
        path = self.tmp / "swap.db"
        build_database(path, 1, ponds=3)
        # 换名提交后、写版本号前的不变量阶段“崩溃”
        me.set_faults({"at_phase": "invariants"})
        result = self.migrate(path)
        self.assertEqual(result.state, "interrupted")

        me.set_faults({})
        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assertEqual(meta_value(path, "version"), "3")
        self.assert_ready_v3(path, 3)

    def test_post_upgrade_invariant_violation_blocks_never_downgrades(self):
        """换表已提交为 v3 结构但数据违反不变量 -> 人工介入，且绝不自动降级。"""
        path = self.tmp / "invariant.db"
        build_database(path, 2, ponds=2)

        # 手工构造“结构=v3、数据违反 species 非空不变量、version 未落盘”的现场。
        # 不建 pond_code 唯一索引对该不变量无影响，但为还原 v3 结构摘要，
        # 这里先建完整 v3 结构，再用不触发唯一约束的方式写入空 species。
        con = sqlite3.connect(str(path))
        con.execute("DROP TABLE ponds")
        v3_sql = schema.V3_PONDS_SQL.replace("ponds__new", "ponds")
        con.execute(v3_sql)
        con.execute(schema.V3_POND_CODE_INDEX[1])
        con.execute("CREATE INDEX ix_ponds_id ON ponds (id)")
        con.execute("CREATE UNIQUE INDEX ix_ponds_name ON ponds (name)")
        con.execute(
            "INSERT INTO ponds(id,name,area,water_depth,species,status,created_at,"
            "updated_at,location_code,pond_code) VALUES "
            "(1,'a',1,1,'','active','2026','2026','','P000001'),"
            "(2,'b',1,1,'s','active','2026','2026','','P000002')"
        )
        con.execute(
            "INSERT OR REPLACE INTO schema_meta(key,value) VALUES('version','2')"
        )
        con.execute(
            "INSERT OR REPLACE INTO schema_meta(key,value) VALUES('phase','swap')"
        )
        con.commit()
        con.close()

        result = self.migrate(path)
        self.assertEqual(result.state, "blocked")
        self.assertIn("不变量", result.detail)
        # 版本号绝不回写为更低、结构也不回退
        self.assertEqual(meta_value(path, "version"), "2")
        status = me.inspect_status(make_engine(path))
        self.assertEqual(status["schema"], "blocked")

        # 备份文件保留，可人工恢复；后续重启仍明确拒绝而非猜测
        again = self.migrate(path)
        self.assertEqual(again.state, "blocked")


class FutureVersionTests(MigrationTestBase):
    def test_unknown_future_version_refuses_startup(self):
        path = self.tmp / "future.db"
        con = sqlite3.connect(str(path))
        schema.build_schema_at(con, CURRENT_VERSION)
        con.execute(
            "CREATE TABLE IF NOT EXISTS schema_meta "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        con.execute("INSERT INTO schema_meta(key,value) VALUES('version','99')")
        con.commit()
        con.close()

        result = self.migrate(path)
        self.assertEqual(result.state, "future_version")
        self.assertEqual(result.version, 99)
        self.assertIn("禁止降级", result.detail)

        status = me.inspect_status(make_engine(path))
        self.assertEqual(status["schema"], "future_version")
        # 再来一次依旧拒绝，且不改动数据
        self.assertEqual(self.migrate(path).state, "future_version")
        self.assertEqual(meta_value(path, "version"), "99")


class RestartRetentionTests(MigrationTestBase):
    def test_crash_between_v2_alter_and_version_write_is_recognized(self):
        """加列已提交、version 未落盘：结构摘要识别为 v2 并继续升到 v3。"""
        path = self.tmp / "v2crash.db"
        # 直接造一个“v2 结构但无版本表”的库
        con = sqlite3.connect(str(path))
        for sql in schema.V2_TABLES.values():
            con.execute(sql)
        for _, sql in schema.V1_INDEXES:
            con.execute(sql)
        con.execute(
            "INSERT INTO ponds(id,name,area,water_depth,species,status,created_at,"
            "updated_at,location_code) VALUES (1,'塘',1,2,'草鱼','active','2026','2026','L1')"
        )
        con.commit()
        con.close()

        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assertEqual(meta_value(path, "version"), str(CURRENT_VERSION))

    def test_stale_blocked_lock_clears_after_manual_repair(self):
        """运维就地修复使结构达标后，陈旧的 blocked 锁在启动时自动清除。"""
        path = self.tmp / "repaired.db"
        # 先制造一个 blocked 现场（v3 结构 + 空 species + blocked 锁）
        con = sqlite3.connect(str(path))
        schema.build_schema_at(con, 3)
        con.execute(
            "CREATE TABLE IF NOT EXISTS schema_meta "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        con.execute("INSERT INTO schema_meta(key,value) VALUES('version','3')")
        con.execute(
            "CREATE TABLE schema_migration_lock (id INTEGER PRIMARY KEY CHECK(id=1),"
            "owner TEXT, state TEXT, heartbeat TEXT, target_version INTEGER,"
            "note TEXT, acquired_at TEXT)"
        )
        con.execute(
            "INSERT INTO ponds(id,name,area,water_depth,species,status,created_at,"
            "updated_at,location_code,pond_code) VALUES "
            "(1,'a',1,1,'','active','2026','2026','','P000001')"
        )
        con.execute(
            "INSERT INTO schema_migration_lock VALUES(1,'dead','blocked','2020-01-01T00:00:00+00:00',3,'不变量失败','2020-01-01T00:00:00+00:00')"
        )
        con.execute(
            "INSERT INTO schema_meta(key,value) VALUES('blocked_reason','不变量失败')"
        )
        con.commit()
        con.close()

        # 运维修复数据
        con = sqlite3.connect(str(path))
        con.execute("UPDATE ponds SET species='草鱼' WHERE id=1")
        con.commit()
        con.close()

        result = self.migrate(path)
        self.assertEqual(result.state, "ready", result.detail)
        self.assertEqual(meta_value(path, "version"), str(CURRENT_VERSION))
        self.assertIsNone(meta_value(path, "blocked_reason"))

    def test_restart_retains_database_and_serves_existing_data(self):
        path = self.tmp / "retain.db"
        build_database(path, 1, ponds=5, batches=2)

        self.assert_ready_v3(path, 5)
        # 模拟“保留数据库文件重启”：全新引擎对象、同一文件
        self.assert_ready_v3(path, 5)

        # 重启后用 ORM 读到迁移前的旧数据
        from sqlalchemy.orm import Session
        from app.models import Batch, Pond
        with Session(make_engine(path)) as db:
            self.assertEqual(db.query(Pond).count(), 5)
            self.assertEqual(db.query(Batch).count(), 2)
            old = db.query(Pond).filter(Pond.id == 2).one()
            self.assertEqual(old.species, "未指定品种")
            self.assertEqual(old.pond_code, "P000002")


if __name__ == "__main__":
    unittest.main()
