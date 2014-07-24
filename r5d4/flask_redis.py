from flask import current_app
import redis


def connect_redis(host, port, db):
    """
    >>> from r5d4.test_settings import (REDIS_HOST, REDIS_PORT, CONFIG_DB)
    >>> connect_redis(REDIS_HOST, REDIS_PORT, CONFIG_DB) is not None
    True
    """
    try:
        r = redis.StrictRedis(host=host, port=port, db=db)
        return r
    except redis.exceptions.ConnectionError:
        return None


def get_conf_db(app=current_app, exclusive=False):
    if not exclusive and hasattr(app, 'conf_db'):
        return app.conf_db
    else:
        new_conn = connect_redis(host=app.config["REDIS_HOST"],
                                 port=app.config["REDIS_PORT"],
                                 db=app.config["CONFIG_DB"])
        if not exclusive:
            app.conf_db = new_conn
        return new_conn


def get_data_db(data_db=None, app=current_app):
    if data_db is None:
        data_db = app.config["DEFAULT_DATA_DB"]
    return connect_redis(host=app.config["REDIS_HOST"],
                         port=app.config["REDIS_PORT"],
                         db=data_db)
