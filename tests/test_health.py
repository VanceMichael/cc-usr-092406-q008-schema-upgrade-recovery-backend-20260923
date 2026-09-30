"""健康接口与流量门控的 HTTP 级测试。

三个层次必须分开表达：
* ``/health/live`` —— 进程存活，永远 200（即使数据库结构未就绪/被拒绝）；
* ``/health/ready`` 与 ``/health`` —— 进程存活 + 数据库可用 + 结构就绪；
* 结构未就绪时 ``/api/*`` 一律 503，并给出清晰状态码。
"""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from db_fixtures import build_database

from app.main import create_app
from app.migrations import CURRENT_VERSION, schema


class HealthTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def client_for(self, name):
        path = self.tmp / name
        return self._client_at(path)

    def _client_at(self, path):
        from sqlalchemy import create_engine

        eng = create_engine(
            f"sqlite:///{path}", connect_args={"check_same_thread": False}
        )
        return TestClient(create_app(eng)), path


class FreshAndUpgradedHealthTests(HealthTestBase):
    def test_fresh_database_becomes_ready_and_serves_traffic(self):
        client, _ = self.client_for("fresh.db")
        with client:
            live = client.get("/health/live")
            self.assertEqual(live.status_code, 200)
            self.assertEqual(live.json()["process"], "up")

            ready = client.get("/health/ready")
            self.assertEqual(ready.status_code, 200)
            body = ready.json()
            self.assertEqual(body["status"], "ready")
            self.assertEqual(body["checks"], {"process": "up", "database": "up",
                                              "schema": "ready"})
            self.assertEqual(body["schema_version"], CURRENT_VERSION)

            # /health 旧路径与 /health/ready 同语义
            self.assertEqual(client.get("/health").status_code, 200)

            r = client.post("/api/ponds/", json={
                "name": "一号塘", "area": 10, "water_depth": 2, "species": "草鱼",
            })
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["pond_code"], "P000001")

    def test_old_v1_database_is_upgraded_before_serving(self):
        path = self.tmp / "old.db"
        build_database(path, 1, ponds=3, batches=1)
        client, _ = self._client_at(path)
        with client:
            ready = client.get("/health/ready")
            self.assertEqual(ready.status_code, 200, ready.text)
            self.assertEqual(ready.json()["checks"]["schema"], "ready")
            ponds = client.get("/api/ponds/").json()
            self.assertEqual(len(ponds), 3)
            self.assertEqual(ponds[0]["pond_code"], "P000001")


class BlockedTrafficGatingTests(HealthTestBase):
    def test_api_returns_clear_503_for_each_unready_phase(self):
        client, _ = self.client_for("states.db")
        # 不进入 with，避免触发 startup 迁移；直接驱动进程内迁移状态
        cases = {
            "starting": "service_starting",
            "running": "schema_migrating",
            "blocked": "migration_blocked",
            "future_version": "unsupported_future_version",
        }
        for phase, code in cases.items():
            client.app.state.migration.phase = phase
            r = client.get("/api/ponds/")
            self.assertEqual(r.status_code, 503, phase)
            self.assertEqual(r.json()["code"], code, phase)
            # 健康面不受门控影响，且存活始终为 up
            self.assertEqual(client.get("/health/live").status_code, 200)

    def _future_version_db(self, path):
        con = sqlite3.connect(str(path))
        schema.build_schema_at(con, CURRENT_VERSION)
        con.execute(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        con.execute("INSERT INTO schema_meta(key,value) VALUES('version','42')")
        con.commit()
        con.close()

    def test_live_stays_up_while_ready_and_api_reject_future_version(self):
        path = self.tmp / "future.db"
        self._future_version_db(path)
        client, _ = self._client_at(path)
        with client:
            # 进程依然存活
            live = client.get("/health/live")
            self.assertEqual(live.status_code, 200)

            # 但结构就绪检查失败：503 + 分项状态
            ready = client.get("/health/ready")
            self.assertEqual(ready.status_code, 503)
            body = ready.json()
            self.assertEqual(body["checks"]["process"], "up")
            self.assertEqual(body["checks"]["database"], "up")
            self.assertEqual(body["checks"]["schema"], "future_version")
            self.assertIn("禁止降级", body["detail"])

            # 业务流量被清晰拒绝
            r = client.get("/api/ponds/")
            self.assertEqual(r.status_code, 503)
            self.assertEqual(r.json()["code"], "unsupported_future_version")
            self.assertEqual(r.headers.get("retry-after"), "2")

            # 非业务面仍可达
            self.assertEqual(client.get("/").status_code, 200)

    def test_database_down_is_distinct_from_process_alive(self):
        from sqlalchemy import create_engine

        bad = create_engine(
            "sqlite:////nonexistent/directory/that/does/not/exist.db",
            connect_args={"check_same_thread": False},
        )
        client = TestClient(create_app(bad))
        # 不触发 startup；直接探测
        live = client.get("/health/live")
        self.assertEqual(live.status_code, 200)
        ready = client.get("/health/ready")
        self.assertEqual(ready.status_code, 503)
        checks = ready.json()["checks"]
        self.assertEqual(checks["process"], "up")
        self.assertEqual(checks["database"], "down")
        self.assertNotEqual(checks["schema"], "ready")


if __name__ == "__main__":
    unittest.main()
