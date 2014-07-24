#!/usr/bin/env python
from __future__ import absolute_import
import unittest
import doctest
import os
import r5d4
from r5d4 import app
from r5d4.analytics_worker import start_analytics_worker
from r5d4.analytics_manager import AnalyticsManager
from r5d4.flask_redis import get_conf_db
from r5d4.test_settings import REDIS_HOST, REDIS_PORT, CONFIG_DB


def load_tests(loader, tests, ignore):
    # Loading doctests from modules
    tests.addTests(doctest.DocTestSuite(r5d4.flask_redis))
    tests.addTests(doctest.DocTestSuite(r5d4.utility))
    tests.addTests(doctest.DocTestSuite(r5d4.mapping_functions))
    tests.addTests(doctest.DocTestSuite(r5d4.analytics_browser))
    return tests


def make_absolute_path(relative_path):
    ROOT_DIR = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(ROOT_DIR, relative_path))


class r5d4TestCase(unittest.TestCase):
    def setUp(self):
        app.config["TESTING"] = True
        app.config["REDIS_HOST"] = REDIS_HOST
        app.config["REDIS_PORT"] = REDIS_PORT
        app.config["CONFIG_DB"] = CONFIG_DB
        self.conf_db = get_conf_db(app)
        self.conf_db.flushall()
        self.flask_app = app
        self.app = app.test_client()
        self.analytics_worker = start_analytics_worker(app=app)
        self.analytics_manager = AnalyticsManager(app=app)

    def test_r5d4(self):
        # TODO
        pass

    def tearDown(self):
        if self.analytics_worker:
            if self.analytics_worker.is_alive():
                self.analytics_worker.terminate()
            self.analytics_worker.join()
        self.conf_db.flushall()


if __name__ == "__main__":
    unittest.main()
