import typing as t

from sqlalchemy import create_engine, engine
from sqlalchemy.engine import url as engine_url

from paralyze.config import mssql as config
from paralyze.db import json


def get_engine(cfg: config.Config) -> engine.Engine:
    connect_args: t.Dict[str, t.Any]
    query: t.Dict[str, t.Any]

    match cfg.driver:
        case "turbodbc":
            connect_args = dict(
                connect_timeout=cfg.connect_timeout,
                Encrypt="no",
            )
            query = dict(
                driver="ODBC Driver 18 for SQL Server",
            )
        case "pymssql":
            connect_args = dict(
                login_timeout=cfg.connect_timeout,
                timeout=cfg.read_timeout or 0,
            )
            query = dict()
        case _:
            raise ValueError(f"Unknown driver: {cfg.driver}")

    url = engine_url.URL.create(
        drivername=f"mssql+{cfg.driver}",
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        query=query,
    )

    return create_engine(
        url,
        max_overflow=cfg.pool.overflow,
        pool_pre_ping=cfg.pool.pre_ping,
        pool_recycle=cfg.pool.recycle,
        pool_size=cfg.pool.size,
        pool_timeout=cfg.pool.timeout,
        json_serializer=json.dumps,
        connect_args=connect_args,
        echo=cfg.echo,
    )
