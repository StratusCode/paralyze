import typing as t

from . import db

__all__ = (
    "Config",
)


class Base(db.BaseSQL):
    # choices are turbodbc | pymssql
    driver: str = "pymssql"

    schema: str = "dbo"


class Host(Base):
    """
    Holds the configuration for a TCP connection to a MySQL database.
    """

    host: str
    port: int = 1433

    # TODO: think about SSL


Config: t.TypeAlias = Host
