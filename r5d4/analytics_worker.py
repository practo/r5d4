#!/usr/bin/env python
from __future__ import absolute_import
import sys
import traceback
from flask import json
import signal
from multiprocessing import Process
from r5d4.analytics import Analytics
from r5d4.mapping_functions import MEASURING_FUNCTIONS_MAP,\
    DIMENSION_PARSERS_MAP
from r5d4.flask_redis import get_conf_db, get_data_db
from r5d4.utility import construct_key
from r5d4.logger import get_worker_log
from r5d4 import app


def actual_worker(analytics_name, sub, app):
    log = get_worker_log(analytics_name)
    try:
        conf_db = get_conf_db(app, exclusive=True)
        defn = conf_db.get("Analytics:ByName:%s" % analytics_name)
        analytics = Analytics(defn)
        if analytics["data_db"]:
            data_db = get_data_db(analytics["data_db"], app=app)
        else:
            data_db = get_data_db(app=app)
        measures = set(analytics["measures"])
        query_dimensions = set(analytics["query_dimensions"])
        slice_dimensions = set(analytics["slice_dimensions"])
        mapping = analytics["mapping"]
        for content in sub.listen():
            if content["type"] == "message":
                try:
                    data = json.loads(content["data"])
                    transaction = data["payload"]
                    tr_type = data["tr_type"]

                    snoq_dimensions = slice_dimensions - query_dimensions
                    qnos_dimensions = query_dimensions - slice_dimensions

                    def build_key_str(dimensions):
                        key = []
                        for dimension in sorted(list(dimensions)):
                            d_type = mapping[dimension]["type"]
                            function = DIMENSION_PARSERS_MAP[d_type]
                            field = mapping[dimension]["field"]
                            key.append(dimension)
                            key.append(function(transaction[field]))
                        return construct_key(key)

                    query_key_str = build_key_str(query_dimensions)
                    slice_key_str = build_key_str(slice_dimensions)
                    snoq_key_str = build_key_str(snoq_dimensions)

                    # Updating Reference count for qnos dimensions
                    for dimension in sorted(list(qnos_dimensions)):
                        field = mapping[dimension]["field"]
                        ref_count_key = construct_key('RefCount',
                                                      slice_key_str,
                                                      dimension)
                        if tr_type == "insert":
                            value = data_db.hincrby(ref_count_key,
                                                    transaction[field],
                                                    1)
                        elif tr_type == "delete":
                            value = data_db.hincrby(ref_count_key,
                                                    transaction[field],
                                                    -1)
                            if value == 0:
                                data_db.hdel(ref_count_key, transaction[field])

                    # Each measure gets added one at a time
                    for m in measures:
                        if mapping[m]["resource"] != content["channel"]:
                            continue
                        key_str = construct_key(m, query_key_str, snoq_key_str)
                        function = MEASURING_FUNCTIONS_MAP[mapping[m]["type"]]
                        field = mapping[m].get("field", None)
                        conditions = mapping[m].get("conditions", [])
                        kwargs = {
                            "key_str": key_str,
                        }

                        for condition in conditions:
                            condition_field = condition["field"]
                            equals = condition.get("equals", None)
                            not_equals = condition.get("not_equals", None)
                            if equals is not None:
                                if transaction[condition_field] != equals:
                                    break  # Failed equals condition
                            elif not_equals is not None:
                                if transaction[condition_field] == not_equals:
                                    break  # Failed not equals condition
                        else:
                            # All conditions passed
                            if field is not None:
                                kwargs["field_val"] = transaction[field]
                            function(data_db, tr_type, **kwargs)

                except Exception, e:
                    log.error("Error while consuming transaction.\n%s" %
                              traceback.format_exc())
                    log.debug("Resource was: %s" % content["channel"])
                    log.debug("Data was: %s" % json.dumps(data))
    except Exception, e:
        log.critical("Worker crashed.\nError was: %s" % str(e))
        log.debug("Traceback: %s" % traceback.format_exc())
        signal.pause()


class AnalyticsWorker():
    def __init__(self, app):
        self.app = app
        self.conf_db = get_conf_db(app, exclusive=True)
        self.proc = {}
        self.subs = {}
        self.log = get_worker_log('master')

        signal.signal(signal.SIGTERM, self.termination_handler)
        signal.signal(signal.SIGINT, self.termination_handler)
        signal.signal(signal.SIGCHLD, self.child_handler)

    def create_worker(self, a_name):
        sub = self.conf_db.pubsub()
        sub.subscribe(self.conf_db.smembers(
            "Analytics:ByName:%s:Subscriptions" % a_name
        ))
        self.subs[a_name] = sub

        prev_chld_handler = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        prev_term_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        prev_int_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)

        self.log.info("Creating worker for %s" % a_name)
        p = Process(target=actual_worker, args=(a_name, sub, self.app))
        p.start()

        signal.signal(signal.SIGTERM, prev_term_handler)
        signal.signal(signal.SIGINT, prev_int_handler)
        signal.signal(signal.SIGCHLD, prev_chld_handler)

        self.proc[a_name] = p

    def destroy_worker(self, a_name):
        self.log.info("%s is getting deleted" % a_name)
        prev_chld_handler = signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        p = self.proc[a_name]
        del self.proc[a_name]
        p.terminate()
        p.join()
        self.subs[a_name].unsubscribe()
        del self.subs[a_name]
        signal.signal(signal.SIGCHLD, prev_chld_handler)

    def update_analytics(self):
        old_a_name = set(self.subs.keys())
        new_a_name = set(self.conf_db.smembers('Analytics:Active'))

        add_a_name = new_a_name - old_a_name
        for a_name in add_a_name:
            self.create_worker(a_name)

        rem_a_name = old_a_name - new_a_name
        for a_name in rem_a_name:
            self.destroy_worker(a_name)

        for a_name in old_a_name & new_a_name:
            old_subs = self.subs[a_name].channels
            new_subs = self.conf_db.smembers(
                "Analytics:ByName:%s:Subscriptions" % a_name
            )
            add_sub = new_subs - old_subs
            for s in add_sub:
                self.subs[a_name].subscribe(s)
            rem_sub = old_subs - new_subs
            for s in rem_sub:
                self.subs[a_name].unsubscribe(s)
                if self.subs[a_name].subscription_count == 0:
                    self.destroy_worker(a_name)

    def start(self):
        for a_name in self.conf_db.smembers('Analytics:Active'):
            self.create_worker(a_name)
        self.log.info("Listening on 'AnalyticsWorkerCmd' channel")
        commands = self.conf_db.pubsub()
        commands.subscribe('AnalyticsWorkerCmd')
        for cmd in commands.listen():
            if cmd['type'] == 'message':
                self.log.debug('Received %s', cmd['data'])
                if cmd["data"].lower() == "refresh":
                    self.update_analytics()
                self.log.info("AnalyticsWorker refreshed")

    def __del__(self):
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        for p in self.proc.values():
            if p.is_alive():
                p.terminate()
            p.join()

    def child_handler(self, sig_val, ret_code):
        for a_name in self.proc.keys():
            if not self.proc[a_name].is_alive():
                self.log.warn(
                    "Worker Process for %s is not alive, respawning" % a_name
                )
                self.destroy_worker(a_name)
                self.create_worker(a_name)

    def termination_handler(self, sig_val, ret_code):
        self.log.info("Analytics Worker is shutting down")
        self.__del__()
        sys.exit(0)


def start_analytics_worker(app):
    def aw_start():
        AnalyticsWorker(app).start()

    p = Process(target=aw_start)
    p.start()
    return p


if __name__ == "__main__":
    p = start_analytics_worker(app=app)
    try:
        p.join()
    except KeyboardInterrupt:
        if p.is_alive():
            p.terminate()
