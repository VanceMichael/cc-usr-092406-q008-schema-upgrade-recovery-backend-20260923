"""每个结构版本的权威 DDL、前置版本与不变量定义。

版本历史
========

* ``v1`` —— 最初上线版本，无版本表（历史遗留库）。9 张业务表，SQLAlchemy
  ``create_all`` 直接建表，无版本记录。
* ``v2`` —— 在 ``ponds`` 增加 ``location_code``、在 ``batches`` 增加 ``notes``。
  纯加列，SQLite 的 ``ALTER TABLE ADD COLUMN`` 可直接在事务中完成。
  **前置版本：v1。**
* ``v3`` —— 塘口编码治理：``ponds.pond_code`` 必填且唯一、``ponds.species``
  必填。SQLite 不支持为已存在的列加 NOT NULL / 加唯一约束这类表变更，必须走
  “建新表 → 分批复制 → 校验 → 事务内换名”的复制流程。
  **前置版本：v2。**

每个版本的 *校验摘要* 是对“表 / 列 / 外键 / 索引”结构化信息计算的 sha256，
启动时用真实库重新计算并与这里的常量比对；摘要不符即拒绝自动推进，转人工。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import OrderedDict
from typing import Dict, List, Tuple

# 框架自身的表，计算业务结构摘要时始终排除
META_TABLES = ("schema_meta", "schema_migration_lock")
# 复制换表过程中的临时名
SHADOW_SUFFIX = "__new"
OLD_SUFFIX = "__old"

# ---------------------------------------------------------------------------
# v1：历史基线（与旧版本 SQLAlchemy create_all 产物逐列一致）
# ---------------------------------------------------------------------------

V1_TABLES: "OrderedDict[str, str]" = OrderedDict()

V1_TABLES["ponds"] = """
CREATE TABLE ponds (
	id INTEGER NOT NULL,
	name VARCHAR(100) NOT NULL,
	area FLOAT NOT NULL,
	water_depth FLOAT NOT NULL,
	species VARCHAR(100),
	status VARCHAR(20),
	created_at DATETIME,
	updated_at DATETIME,
	PRIMARY KEY (id)
)
"""

V1_TABLES["batches"] = """
CREATE TABLE batches (
	id INTEGER NOT NULL,
	batch_number VARCHAR(50) NOT NULL,
	pond_id INTEGER NOT NULL,
	species VARCHAR(100) NOT NULL,
	stocking_date DATE NOT NULL,
	estimated_harvest_date DATE,
	actual_harvest_date DATE,
	status VARCHAR(20),
	created_at DATETIME,
	updated_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(pond_id) REFERENCES ponds (id)
)
"""

V1_TABLES["stocking_records"] = """
CREATE TABLE stocking_records (
	id INTEGER NOT NULL,
	batch_id INTEGER NOT NULL,
	species VARCHAR(100) NOT NULL,
	quantity INTEGER NOT NULL,
	source VARCHAR(200),
	batch_number VARCHAR(50),
	weight_per_unit FLOAT,
	total_weight FLOAT,
	notes TEXT,
	created_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(batch_id) REFERENCES batches (id)
)
"""

V1_TABLES["feeding_records"] = """
CREATE TABLE feeding_records (
	id INTEGER NOT NULL,
	batch_id INTEGER NOT NULL,
	feeding_date DATE NOT NULL,
	feed_type VARCHAR(100) NOT NULL,
	feed_quantity FLOAT NOT NULL,
	feeding_time VARCHAR(20),
	weather VARCHAR(50),
	water_temperature FLOAT,
	notes TEXT,
	created_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(batch_id) REFERENCES batches (id)
)
"""

V1_TABLES["water_quality_records"] = """
CREATE TABLE water_quality_records (
	id INTEGER NOT NULL,
	batch_id INTEGER NOT NULL,
	record_date DATE NOT NULL,
	record_time VARCHAR(20),
	water_temperature FLOAT,
	ph_value FLOAT,
	dissolved_oxygen FLOAT,
	ammonia_nitrogen FLOAT,
	nitrite FLOAT,
	transparency FLOAT,
	notes TEXT,
	created_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(batch_id) REFERENCES batches (id)
)
"""

V1_TABLES["medication_records"] = """
CREATE TABLE medication_records (
	id INTEGER NOT NULL,
	batch_id INTEGER NOT NULL,
	medication_date DATE NOT NULL,
	drug_name VARCHAR(200) NOT NULL,
	drug_type VARCHAR(50),
	dosage FLOAT,
	dosage_unit VARCHAR(20),
	administration_method VARCHAR(100),
	purpose VARCHAR(200),
	manufacturer VARCHAR(200),
	batch_number VARCHAR(50),
	notes TEXT,
	created_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(batch_id) REFERENCES batches (id)
)
"""

V1_TABLES["cost_records"] = """
CREATE TABLE cost_records (
	id INTEGER NOT NULL,
	batch_id INTEGER NOT NULL,
	cost_date DATE NOT NULL,
	cost_type VARCHAR(50) NOT NULL,
	amount FLOAT NOT NULL,
	description VARCHAR(500),
	quantity FLOAT,
	unit VARCHAR(20),
	unit_price FLOAT,
	notes TEXT,
	created_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(batch_id) REFERENCES batches (id)
)
"""

V1_TABLES["harvest_sales"] = """
CREATE TABLE harvest_sales (
	id INTEGER NOT NULL,
	batch_id INTEGER NOT NULL,
	sale_date DATE NOT NULL,
	weight FLOAT NOT NULL,
	unit_price FLOAT NOT NULL,
	total_amount FLOAT,
	buyer VARCHAR(200),
	batch_number VARCHAR(50),
	quality_grade VARCHAR(50),
	notes TEXT,
	created_at DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(batch_id) REFERENCES batches (id)
)
"""

# (索引名, 建索引 SQL) —— 与旧版 create_all 的产物一致
V1_INDEXES: List[Tuple[str, str]] = [
    ("ix_ponds_id", "CREATE INDEX ix_ponds_id ON ponds (id)"),
    ("ix_ponds_name", "CREATE UNIQUE INDEX ix_ponds_name ON ponds (name)"),
    ("ix_batches_id", "CREATE INDEX ix_batches_id ON batches (id)"),
    ("ix_batches_batch_number", "CREATE UNIQUE INDEX ix_batches_batch_number ON batches (batch_number)"),
    ("ix_stocking_records_id", "CREATE INDEX ix_stocking_records_id ON stocking_records (id)"),
    ("ix_feeding_records_id", "CREATE INDEX ix_feeding_records_id ON feeding_records (id)"),
    ("ix_water_quality_records_id", "CREATE INDEX ix_water_quality_records_id ON water_quality_records (id)"),
    ("ix_medication_records_id", "CREATE INDEX ix_medication_records_id ON medication_records (id)"),
    ("ix_cost_records_id", "CREATE INDEX ix_cost_records_id ON cost_records (id)"),
    ("ix_harvest_sales_id", "CREATE INDEX ix_harvest_sales_id ON harvest_sales (id)"),
]

# ---------------------------------------------------------------------------
# v2：加列版本
# ---------------------------------------------------------------------------

def _v2_tables() -> "OrderedDict[str, str]":
    tables = OrderedDict((k, v) for k, v in V1_TABLES.items())
    # ALTER TABLE ADD COLUMN 只能把列加到末尾，因此权威 v2 DDL 中列也在末尾，
    # 这样真实“加列得到的 v2 库”与摘要常量逐列一致。
    tables["ponds"] = tables["ponds"].replace(
        "\tupdated_at DATETIME,\n\tPRIMARY KEY (id)\n)",
        "\tupdated_at DATETIME,\n\tlocation_code VARCHAR(20),\n"
        "\tPRIMARY KEY (id)\n)",
    )
    tables["batches"] = tables["batches"].replace(
        "\tupdated_at DATETIME,\n\tPRIMARY KEY (id),\n\tFOREIGN KEY(pond_id)",
        "\tupdated_at DATETIME,\n\tnotes TEXT,\n"
        "\tPRIMARY KEY (id),\n\tFOREIGN KEY(pond_id)",
    )
    return tables


V2_TABLES = _v2_tables()

# v2 在 v1 之上的加列语句（幂等：执行前先查列是否存在）
V2_ALTER_COLUMNS: List[Tuple[str, str]] = [
    ("ponds", "ALTER TABLE ponds ADD COLUMN location_code VARCHAR(20)"),
    ("batches", "ALTER TABLE batches ADD COLUMN notes TEXT"),
]

# ---------------------------------------------------------------------------
# v3：塘口编码治理（需要重建 ponds 表）
# ---------------------------------------------------------------------------

V3_PONDS_SQL = """
CREATE TABLE ponds__new (
	id INTEGER NOT NULL,
	name VARCHAR(100) NOT NULL,
	area FLOAT NOT NULL,
	water_depth FLOAT NOT NULL,
	species VARCHAR(100) NOT NULL,
	status VARCHAR(20),
	created_at DATETIME,
	updated_at DATETIME,
	location_code VARCHAR(20),
	pond_code VARCHAR(30) NOT NULL DEFAULT '',
	PRIMARY KEY (id)
)
"""

V3_POND_CODE_INDEX = (
    "ix_ponds_pond_code",
    "CREATE UNIQUE INDEX ix_ponds_pond_code ON ponds (pond_code)",
)

# 复制数据时为历史塘口补编码与品种
V3_COPY_SQL = """
INSERT INTO ponds__new (
    id, name, area, water_depth, species, status, created_at,
    updated_at, location_code, pond_code
)
SELECT
    id, name, area, water_depth,
    COALESCE(species, :default_species),
    status, created_at, updated_at,
    COALESCE(location_code, ''),
    'P' || printf('%06d', id)
FROM ponds
WHERE id > :last_id
ORDER BY id
LIMIT :batch_size
"""

# v3 升级后的业务不变量（SQL 返回 0 行表示通过；返回违例行说明结构就绪但数据不合法）
V3_INVARIANTS: List[Tuple[str, str]] = [
    ("pond_code 不为空", "SELECT id FROM ponds WHERE pond_code IS NULL OR pond_code = ''"),
    ("species 不为空", "SELECT id FROM ponds WHERE species IS NULL OR species = ''"),
    ("pond_code 无重复", "SELECT pond_code FROM ponds GROUP BY pond_code HAVING COUNT(*) > 1"),
    ("无悬挂外键批次", """
        SELECT b.id FROM batches b LEFT JOIN ponds p ON p.id = b.pond_id WHERE p.id IS NULL
    """),
]

VERSION_INFO: Dict[int, Dict[str, object]] = {
    1: {
        "predecessor": None,
        "description": "历史基线：create_all 建表，无版本记录",
        "post_invariants": [],
    },
    2: {
        "predecessor": 1,
        "description": "ponds.location_code、batches.notes 加列",
        "post_invariants": [],
    },
    3: {
        "predecessor": 2,
        "description": "ponds.pond_code 必填唯一、ponds.species 必填（重建表）",
        "post_invariants": V3_INVARIANTS,
    },
}


def _build_database(
    conn: sqlite3.Connection,
    tables: "OrderedDict[str, str]",
    indexes: List[Tuple[str, str]],
) -> None:
    for sql in tables.values():
        conn.execute(sql)
    for _, sql in indexes:
        conn.execute(sql)


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]


def table_fingerprint(conn: sqlite3.Connection, table: str) -> str:
    """单表结构摘要：列定义 + 外键 + 挂在该表上的索引（含列序与唯一性）。"""
    cols = [
        [r[0], r[1], (r[2] or "").upper(), r[3], r[4], r[5]]
        for r in conn.execute(f'PRAGMA table_info("{table}")')
    ]
    fks = [
        [r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]]
        for r in conn.execute(f'PRAGMA foreign_key_list("{table}")')
    ]
    indexes = []
    for row in conn.execute(f'PRAGMA index_list("{table}")'):
        idx_name = row[1]
        unique = row[2]
        idx_cols = [c[2] for c in conn.execute(f'PRAGMA index_info("{idx_name}")')]
        indexes.append([idx_name, unique, idx_cols])
    payload = json.dumps(
        {"columns": cols, "foreign_keys": fks, "indexes": sorted(indexes)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def schema_fingerprint(conn: sqlite3.Connection) -> Tuple[str, Dict[str, str]]:
    """整个业务结构的摘要；自动忽略版本表与迁移临时表。"""
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    per_table: Dict[str, str] = {}
    for name in names:
        if name in META_TABLES or name.endswith((SHADOW_SUFFIX, OLD_SUFFIX)):
            continue
        per_table[name] = table_fingerprint(conn, name)
    overall = hashlib.sha256(
        json.dumps(per_table, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return overall, per_table


def build_schema_at(conn: sqlite3.Connection, version: int) -> None:
    """在给定连接上从零建出 *version* 的完整业务结构（不含版本表）。"""
    if version == 1:
        _build_database(conn, V1_TABLES, V1_INDEXES)
    elif version == 2:
        _build_database(conn, V2_TABLES, V1_INDEXES)
    elif version == 3:
        tables = OrderedDict((k, v) for k, v in V2_TABLES.items())
        tables["ponds"] = V3_PONDS_SQL.replace("ponds__new", "ponds")
        indexes = list(V1_INDEXES) + [V3_POND_CODE_INDEX]
        _build_database(conn, tables, indexes)
    else:
        raise ValueError(f"未知结构版本: {version}")


def _compute_expected_fingerprints() -> Dict[int, str]:
    """在内存库中按权威 DDL 建出每一版，得到各版本应有的结构摘要常量。"""
    expected: Dict[int, str] = {}
    for version, tables in ((1, V1_TABLES), (2, V2_TABLES)):
        mem = sqlite3.connect(":memory:")
        _build_database(mem, tables, V1_INDEXES)
        digest, _ = schema_fingerprint(mem)
        expected[version] = digest
        mem.close()

    # v3：ponds 换成重建后的形态，并增加 pond_code 唯一索引
    mem = sqlite3.connect(":memory:")
    tables = OrderedDict((k, v) for k, v in V2_TABLES.items())
    tables["ponds"] = V3_PONDS_SQL.replace("ponds__new", "ponds")
    indexes = list(V1_INDEXES) + [V3_POND_CODE_INDEX]
    _build_database(mem, tables, indexes)
    digest, _ = schema_fingerprint(mem)
    expected[3] = digest
    mem.close()
    return expected


EXPECTED_FINGERPRINTS = _compute_expected_fingerprints()
