from flask import current_app
import redis


def connect_redis(unix_socket_path, host, port, db):
    """
    >>> connect_redis(RTC["unix_socket_path"], RTC["host"], RTC["port"],
    >>>         CONFIG_DB) is not None
    True

    >>> connect_redis("/tmp/unknown.sock", RTC["host"], RTC["port"],
    >>>         CONFIG_DB) is not None
    True

    >>> connect_redis(RTC["unix_socket_path"], "unknown", 666,
    >>>         CONFIG_DB) is not None
    True

    >>> connect_redis("/tmp/unknown.sock", "unknown", 666,
    >>>         CONFIG_DB) is not None
    False
    """

    # Try connecting through UNIX socket
    settings = {
        "unix_socket_path": unix_socket_path,
        "db": db
    }
    try:
        r = redis.Redis(**settings)
        r.ping()
        return r
    except redis.exceptions.ConnectionError:
        pass

    # Fallback, try TCP connection
    settings = {
        "host": host,
        "port": port,
        "db": db
    }
    try:
        r = redis.Redis(**settings)
        r.ping()
        return r
    except redis.exceptions.ConnectionError:
        # No more fallbacks
        return None


def get_conf_db(app=current_app, exclusive=False):
    if not exclusive and hasattr(app, 'conf_db'):
        return app.conf_db
    else:
        new_conn = connect_redis(
            unix_socket_path=app.config["REDIS_UNIX_SOCKET_PATH"],
            host=app.config["REDIS_HOST"],
            port=app.config["REDIS_PORT"],
            db=app.config["CONFIG_DB"]
        )
        if not exclusive:
            app.conf_db = new_conn
        return new_conn


def get_data_db(data_db=None, app=current_app):
    if data_db is None:
        data_db = app.config["DEFAULT_DATA_DB"]
    return connect_redis(
        unix_socket_path=app.config["REDIS_UNIX_SOCKET_PATH"],
        host=app.config["REDIS_HOST"],
        port=app.config["REDIS_PORT"],
        db=data_db
    )
