"""检查项目基线的入口、路由装配和本地存储约定。"""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class BaselineTest(unittest.TestCase):
    def test_backend_uses_sqlite_and_registers_domain_routers(self):
        database = (ROOT / "backend/app/database.py").read_text(encoding="utf-8")
        main = (ROOT / "backend/app/main.py").read_text(encoding="utf-8")
        self.assertIn("sqlite:///", database)
        for name in ("ponds", "batches", "stocking", "feeding", "water_quality", "medication", "costs", "harvest", "analysis"):
            self.assertIn(f"{name}.router", main)

    def test_backend_only_snapshot_has_documented_checks(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertFalse((ROOT / "client_ui").exists())
        self.assertFalse((ROOT / "frontend").exists())
        self.assertIn("python3 -m unittest discover -s tests -v", readme)
        self.assertIn("python3 -m compileall -q backend/app", readme)


if __name__ == "__main__":
    unittest.main()
