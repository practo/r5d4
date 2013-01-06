#!/usr/bin/env python
from __future__ import absolute_import
import sys
from r5d4.analytics import Analytics
from r5d4.settings import REDIS_UNIX_SOCKET_PATH, REDIS_HOST, REDIS_PORT, \
    CONFIG_DB
from r5d4.flask_redis import get_conf_db
from r5d4 import run


class AnalyticsManager:
    def __init__(self, app):
        self.cdb = get_conf_db(app)

    def load_analytics(self, analytics, db=None):
        name = analytics["name"]
        if db is not None:
            analytics.set_data_db(int(db))
        self.cdb.set("Analytics:ByName:%s" % name, analytics.json_serialize())
        for measure in analytics["measures"]:
            resource = analytics["mapping"][measure]["resource"]
            self.cdb.sadd("Analytics:ByName:%s:Subscriptions" % name, resource)
            self.cdb.sadd("Subscriptions:%s:ActiveAnalytics" % resource, name)
        self.cdb.sadd("Analytics:Active", name)
        self.cdb.publish("AnalyticsWorkerCmd", "refresh")

    def dump_analytics(self, a_name=None):
        if a_name is None:
            a_names = self.cdb.smembers("Analytics:Active")
            for a in a_names:
                self.dump_analytics(a)
        else:
            analytics = Analytics(self.cdb.get("Analytics:ByName:%s" % a_name))
            analytics.json_serialize(open("%s.json" % a_name, 'w'), indent=2)

    def disable_analytics(self, a_name):
        self.cdb.srem("Analytics:Active", a_name)
        subs = self.cdb.smembers("Analytics:ByName:%s:Subscriptions" % a_name)
        for sub in subs:
            self.cdb.srem("Subscriptions:%s:ActiveAnalytics" % sub, a_name)
        self.cdb.publish("AnalyticsWorkerCmd", "refresh")

    def enable_analytics(self, a_name):
        if not self.cdb.exists("Analytics:ByName:%s" % a_name):
            sys.stderr.write(
                "Analytics is not loaded.\n"
                "Use 'load' command and the analytics json file\n"
            )
        self.cdb.sadd("Analytics:Active", a_name)
        subs = self.cdb.smembers("Analtyics:ByName:%s:Subscriptions" % a_name)
        for sub in subs:
            self.cdb.sadd("Subscriptions:%s:ActiveAnalytics" % sub, a_name)
        self.cdb.publish("AnalyticsWorkerCmd", "refresh")

    def display_usage(self):
        sys.stdout.write("""
        Usage: %s <command> [<arg>[...]]
        Commands:
        load - Loads one or more analytics from json file and Activates.
        dump - Dumps one or more analytics given by name back to json file.
        dumpall - Dumps all analytics. No args is required.
        disable - Disables one or more analytics given by name.
        enable - Enables one or more analytics given by name.
        commands - Display this
        help - Display this\n""" % sys.argv[0])

if __name__ == "__main__":
    run.app.config['CONFIG_DB'] = CONFIG_DB
    run.app.config['REDIS_HOST'] = REDIS_HOST
    run.app.config['REDIS_PORT'] = REDIS_PORT
    run.app.config['REDIS_UNIX_SOCKET_PATH'] = REDIS_UNIX_SOCKET_PATH
    if len(sys.argv) > 1:
        amgr = AnalyticsManager(run.app)
        command = sys.argv[1].lower()
        args = sys.argv[2:]

        if command == "load":
            db = None
            if args[0][0] == '-':
                    db = -1 * int(args[0])
                    args = args[1:]
            for filename in args:
                amgr.load_analytics(Analytics(open(filename, 'r').read()), db)
        elif command == "dump":
            for a_name in args:
                amgr.dump_analytics(a_name)
        elif command == "dumpall":
            amgr.dump_analytics()
        elif command == "disable":
            for a_name in args:
                amgr.disable_analytics(a_name)
        elif command == "enable":
            for a_name in args:
                amgr.enable_analytics(a_name)
        elif command == "commands" or command == "help":
            amgr.display_usage()
        else:
            sys.stderr.write("Error: %s is not in the list of commands\n" %
                             command)
            amgr.display_usage()
    else:
        amgr.display_usage()
