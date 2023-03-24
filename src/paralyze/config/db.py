class SQLAlchemyPool:
    """
    Holds the configuration for the MySQL connection pool.
    """

    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_size
    size: int = 10
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.max_overflow
    overflow: int = 5
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_timeout
    timeout: int = 1
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_recycle
    recycle: int = 120
    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.pool_pre_ping
    pre_ping: bool = True


class BaseSQL:
    username: str
    password: str
    database: str

    # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.echo
    echo: bool = False

    pool: SQLAlchemyPool = SQLAlchemyPool()
    # time in seconds to wait for the initial connection to the server
    connect_timeout: int = 5
    # time for writing to the connection in seconds, default no timeout
    write_timeout: int | None = None
    # time for reading from the connection in seconds, default no timeout
    read_timeout: int | None = None


class Host(BaseSQL):
    """
    Holds the configuration for a TCP connection to a MySQL database.
    """

    host: str
    port: int

    # TODO: think about SSL


class Socket(BaseSQL):
    """
    Similar to `Host`, but uses a Unix socket instead of a TCP connection.
    """

    # path to the Unix socket
    socket: str
