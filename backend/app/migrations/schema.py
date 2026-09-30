"""结构指纹、校验摘要与复制校验原语。

指纹覆盖全部业务表（不包含 ``_`` 前缀的迁移记账表与 ``sqlite_`` 内部表）：

* 列：名称、类型、NOT NULL、默认值、主键位置（与 ``PRAGMA table_info`` 一致）；
* 索引：名称、列序、唯一性、来源（CREATE INDEX / UNIQUE 约束自动索引）；
* 外键：列、被引用表与列、级联策略。

对规范化后的结构计算 SHA-256，作为每个版本的“校验摘要”。
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Tuple

BOOKKEEPING_PREFIX = "_"


def _normalize_type(type_str: Optional[str]) -> str:
    """规整类型文本：大写、折叠空白、去掉逗号两侧空格（NUMERIC(10, 2) == NUMERIC(10,2)）。"""
    if not type_str:
        return ""
    collapsed = re.sub(r"\s+", " ", type_str.strip().upper())
    return re.sub(r"\s*,\s*", ",", collapsed)


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    notnull: int = 0
    default: Optional[str] = None
    pk: int = 0


@dataclass(frozen=True)
class Index:
    name: str
    columns: Tuple[str, ...]
    unique: int
    origin: str  # c=CREATE INDEX, u=UNIQUE 约束自动索引, pk=主键索引


@dataclass(frozen=True)
class ForeignKey:
    column: str
    ref_table: str
    ref_column: Optional[str]
    on_update: str
    on_delete: str


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: Tuple[Column, ...]
    indexes: Tuple[Index, ...] = ()
    foreign_keys: Tuple[ForeignKey, ...] = ()


@dataclass(frozen=True)
class SchemaSpec:
    version: int
    tables: Tuple[TableSpec, ...]

    def table(self, name: str) -> TableSpec:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(name)

    def with_table(self, table: TableSpec) -> "SchemaSpec":
        tables = tuple(t for t in self.tables if t.name != table.name) + (table,)
        tables = tuple(sorted(tables, key=lambda t: t.name))
        return SchemaSpec(self.version, tables)

    def with_version(self, version: int) -> "SchemaSpec":
        return SchemaSpec(version, self.tables)


def _canonical(spec: SchemaSpec) -> str:
    payload = {
        "version": spec.version,
        "tables": {
            t.name: {
                "columns": [
                    [c.name, _normalize_type(c.type), c.notnull, c.default, c.pk]
                    for c in t.columns
                ],
                "indexes": [
                    [i.name, list(i.columns), i.unique, i.origin]
                    for i in sorted(t.indexes, key=lambda i: i.name)
                ],
                "foreign_keys": [
                    [fk.column, fk.ref_table, fk.ref_column, fk.on_update, fk.on_delete]
                    for fk in sorted(t.foreign_keys, key=lambda fk: fk.column)
                ],
            }
            for t in sorted(spec.tables, key=lambda t: t.name)
        },
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def schema_digest(spec: SchemaSpec) -> str:
    """返回结构规范的 SHA-256 摘要（版本的校验摘要）。"""
    return hashlib.sha256(_canonical(spec).encode("utf-8")).hexdigest()


def _is_business_table(name: str) -> bool:
    return not name.startswith("sqlite_") and not name.startswith(BOOKKEEPING_PREFIX)


def read_schema(conn: sqlite3.Connection) -> SchemaSpec:
    """从活动数据库读取结构指纹（``version`` 字段取自 PRAGMA user_version）。"""
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    ]
    tables = []
    for name in sorted(n for n in names if _is_business_table(n)):
        columns = tuple(
            Column(
                name=r[1],
                type=_normalize_type(r[2]),
                notnull=r[3],
                default=r[4],
                pk=r[5],
            )
            for r in conn.execute(f'PRAGMA table_info("{name}")')
        )
        indexes = []
        for r in conn.execute(f'PRAGMA index_list("{name}")'):
            idx_name, unique, origin, partial = r[1], r[2], r[3], r[4]
            if partial:
                # 当前版本不使用部分索引；出现则拒绝猜测
                raise ValueError(f"不支持的部分索引: {idx_name}")
            idx_cols = tuple(
                c[2]
                for c in sorted(
                    conn.execute(f'PRAGMA index_info("{idx_name}")').fetchall(),
                    key=lambda c: c[0],
                )
            )
            indexes.append(Index(idx_name, idx_cols, unique, origin))
        fks = tuple(
            ForeignKey(
                column=r[3],
                ref_table=r[2],
                ref_column=r[4],
                on_update=r[5],
                on_delete=r[6],
            )
            for r in conn.execute(f'PRAGMA foreign_key_list("{name}")')
        )
        tables.append(
            TableSpec(name, columns, tuple(sorted(indexes, key=lambda i: i.name)), fks)
        )
    return SchemaSpec(version, tuple(tables))


def live_digest(conn: sqlite3.Connection) -> Tuple[int, str]:
    spec = read_schema(conn)
    return spec.version, schema_digest(spec)


# ------------------------------------------------------------------
# 数据复制校验
# ------------------------------------------------------------------

def table_row_digest(
    conn: sqlite3.Connection,
    table: str,
    columns: Iterable[str],
    where: str = "",
) -> Tuple[int, str]:
    """计算业务表内容的行数与逐行哈希。

    用于复制型迁移的搬运校验：源表与影子表必须行数一致、逐行一致，
    保证不丢行、不重行、主键保持。
    """
    cols = list(columns)
    quoted_cols = ", ".join('"' + c + '"' for c in cols)
    sql = f'SELECT {quoted_cols} FROM "{table}"'
    if where:
        sql += f" WHERE {where}"
    sql += " ORDER BY rowid"
    digest = hashlib.sha256()
    count = 0
    for row in conn.execute(sql):
        count += 1
        digest.update(repr(tuple(row)).encode("utf-8"))
        digest.update(b"\n")
    return count, digest.hexdigest()
