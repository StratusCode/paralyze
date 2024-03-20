from . import db

__all__ = ("Config",)


class Host(db.BaseSQL):
    """
    Holds the configuration for a TCP connection to a MySQL database.
    """

    host: str
    port: int = 3306

    compress: bool = False

    # TODO: think about SSL


class Socket(db.BaseSQL):
    """
    Similar to `Host`, but uses a Unix socket instead of a TCP connection.
    """

    # path to the Unix socket
    socket: str

    compress: bool = False


Config = Host | Socket
