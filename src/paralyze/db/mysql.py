import orjson
from sqlalchemy import create_engine, engine
from sqlalchemy.engine import url as engine_url

from paralyze.config import mysql as config


__all__ = (
    "get_engine",
)


def get_engine(cfg: config.Config) -> engine.Engine:
    driver_name = "mysql+mysqldb"

    match cfg:
        case config.Socket():
            url = engine_url.URL.create(
                drivername=driver_name,
                username=cfg.username,
                password=cfg.password,
                database=cfg.database,
                query={"unix_socket": cfg.socket},
            )
        case config.Host():
            url = engine_url.URL.create(
                drivername=driver_name,
                username=cfg.username,
                password=cfg.password,
                host=cfg.host,
                port=cfg.port,
                database=cfg.database,
            )

    return create_engine(
        url,
        pool_pre_ping=cfg.pool.pre_ping,
        pool_recycle=cfg.pool.recycle,
        pool_size=cfg.pool.size,
        pool_timeout=cfg.pool.timeout,
        max_overflow=cfg.pool.overflow,
        json_serializer=orjson.dumps,
        connect_args=dict(
            connect_timeout=cfg.connect_timeout,
            charset="utf8mb4",
        ),
        echo=cfg.echo,
    )
