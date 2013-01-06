#!/usr/bin/env python
from __future__ import absolute_import
import redis
import sys


class UnsupportedKeyType(Exception):
    def __init__(self, key_type, key):
        self.key_type = key_type
        self.key = key

    def __unicode__(self):
        return "Unsupported type '%s' for key '%s'" % (self.key_type, self.key)

    def __str__(self):
        return self.__unicode__()


class NotANumber(Exception):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __unicode__(self):
        return "Value '%s' for key '%s' is not a number" % (
            self.value,
            self.key
        )

    def __str__(self):
        return self.__unicode__()


def redis_conn(unix_socket_path, db):
    conn = redis.Redis(unix_socket_path=unix_socket_path, db=db)
    conn.ping()
    return conn


def incr_by_float(conn, key, value):
    def incrbyfloat(pipe):
        current_value = float(pipe.get(key))
        new_value = current_value + value
        if new_value == int(new_value):
            new_value = int(new_value)
        pipe.multi()
        pipe.set(key, new_value)
    conn.transaction(incrbyfloat, key)


def hincr_by_float(conn, key, hkey, hval):
    def incrbyfloat(pipe):
        current_value = float(pipe.hget(key, hkey))
        new_value = current_value + hval
        if new_value == int(new_value):
            new_value = int(new_value)
        pipe.multi()
        pipe.hset(key, hkey, new_value)
    conn.transaction(incrbyfloat, key)


def clone_db(source_socket, destination_socket, source_db, destination_db):
    src = redis_conn(source_socket, source_db)
    dest = redis_conn(destination_socket, destination_db)
    dest.flushdb()
    keys = src.keys()
    for key in keys:
        key_type = src.type(key)
        if key_type == "string":
            dest.set(key, src.get(key))
        elif key_type == "set":
            for sval in src.smembers(key):
                dest.sadd(key, sval)
        elif key_type == "hash":
            for hkey, hval in src.hgetall(key).iteritems():
                dest.hset(key, hkey, hval)
        else:
            raise UnsupportedKeyType(key_type, key)


def add_db(source_socket, destination_socket, source_db, destination_db):
    src = redis_conn(source_socket, source_db)
    dest = redis_conn(destination_socket, destination_db)
    keys = src.keys()
    for key in keys:
        key_type = src.type(key)
        if key_type == "string":
            value = src.get(key)
            if dest.exists(key):
                try:
                    float_val = float(value)
                    incr_by_float(dest, key, float_val)
                except ValueError:
                    raise NotANumber(key, value)
            else:
                dest.set(key, value)
        elif key_type == "set":
            value = src.smembers(key)
            for sval in value:
                dest.sadd(key, sval)
        elif key_type == "hash":
            value = src.hgetall(key)
            if dest.exists(key):
                for hkey, hval in value.iteritems():
                    if dest.hexists(key, hkey):
                        try:
                            float_val = float(hval)
                            hincr_by_float(dest, key, hkey, float_val)
                        except ValueError:
                            raise NotANumber("->".join([key, hkey]), hval)
                    else:
                        dest.hset(key, hkey, hval)
            else:
                for hkey, hval in value.iteritems():
                    dest.hset(key, hkey, hval)
        else:
            raise UnsupportedKeyType(key_type, key)


def display_usage():
    sys.stdout.write(
        "Usage %s <source_socket> <destination_socket> <source_db> <temp_db> "
        "<destination_db>\n" % sys.argv[0])
    sys.stdout.write(
        "WARNING: temp_db uses source_socket and existing keys will be "
        "flushed.\n")

if __name__ == "__main__":
    if len(sys.argv) != 6:
        display_usage()
        sys.exit(0)
    source_socket, destination_socket, source_db, temp_db, destination_db = \
        sys.argv[1:]
    clone_db(destination_socket, source_socket, destination_db, temp_db)
    add_db(source_socket, source_socket, source_db, temp_db)
    clone_db(source_socket, destination_socket, temp_db, destination_db)
