"""测试辅助：构造历史版本数据库、连接与建引擎。"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from sqlalchemy import create_engine

from app.migrations import schema  # noqa: E402


def make_engine(path: Path):
    return create_engine(
        f"sqlite:///{path}", connect_args={"check_same_thread": False}
    )


def build_database(path: Path, version: int, *, ponds=0, batches=0) -> Path:
    """在 *path* 建出指定历史版本的库；v1/v2 不写版本表（模拟旧实例）。

    v1 特意写入 species 为 NULL 的塘口，验证 v3 的补码与填充。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    con = sqlite3.connect(str(path))
    if version == 1:
        for sql in schema.V1_TABLES.values():
            con.execute(sql)
        for _, sql in schema.V1_INDEXES:
            con.execute(sql)
    elif version == 2:
        for sql in schema.V2_TABLES.values():
            con.execute(sql)
        for _, sql in schema.V1_INDEXES:
            con.execute(sql)
    else:
        raise ValueError(f"测试夹具只支持历史版本 1/2，收到 {version}")

    for i in range(1, ponds + 1):
        # 偶数 id 的塘口 species 留空，覆盖 COALESCE 填充路径
        species = None if i % 2 == 0 else f"品种{i}"
        con.execute(
            "INSERT INTO ponds(id,name,area,water_depth,species,status,"
            "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (i, f"塘{i:03d}", 10.0 + i, 2.0, species, "active",
             "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
        )
        if version == 2:
            con.execute("UPDATE ponds SET location_code=? WHERE id=?",
                        (f"L{i % 3}", i))
    for i in range(1, batches + 1):
        pond_id = ((i - 1) % max(ponds, 1)) + 1
        con.execute(
            "INSERT INTO batches(id,batch_number,pond_id,species,stocking_date,"
            "status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (i, f"B{i:05d}", pond_id, "罗非鱼", "2026-01-05", "active",
             "2026-01-05 00:00:00", "2026-01-05 00:00:00"),
        )
        if version == 2:
            con.execute("UPDATE batches SET notes=? WHERE id=?", ("旧备注", i))

    if version == 2:
        # v2 是迁移框架产物：真实的 v2 库一定带有版本表与版本号
        con.execute(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        con.execute("INSERT INTO schema_meta(key,value) VALUES('version','2')")
    con.commit()
    con.close()
    return path


def meta_value(path: Path, key: str):
    con = sqlite3.connect(str(path))
    try:
        row = con.execute("SELECT value FROM schema_meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def count_rows(path: Path, table: str) -> int:
    con = sqlite3.connect(str(path))
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()
