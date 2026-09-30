"""数据库引擎与会话工厂。

结构初始化不再在导入时 ``create_all``——结构版本、建表与迁移统一由
:mod:`app.migrations` 在启动流程中完成（见 :func:`app.startup.run_startup`）。
"""

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./aquaculture.db")

if SQLALCHEMY_DATABASE_URL.startswith("sqlite:///"):
    db_file = SQLALCHEMY_DATABASE_URL.replace("sqlite:///", "", 1)
    if db_file and db_file != ":memory:":
        Path(db_file).parent.mkdir(parents=True, exist_ok=True)

_connect_args = {"check_same_thread": False} if SQLALCHEMY_DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
