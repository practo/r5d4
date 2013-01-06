from __future__ import absolute_import
import logging
import r5d4.settings as settings
from logging.handlers import RotatingFileHandler as RFHandler

worker_log_formatter = logging.Formatter(settings.WORKER_LOG_FORMAT,
                                         settings.WORKER_LOG_DATE_FORMAT)
activity_log_formatter = logging.Formatter('%(asctime)s\t%(message)s')


def get_activity_log():
    activity_log = logging.getLogger('r5d4.activity')
    if settings.ACTIVITY_LOG:
        act_log_handler = RFHandler(settings.ACTIVITY_LOG, "a+", 1048576, 15)
    else:
        act_log_handler = logging.StreamHandler()
    activity_log.addHandler(act_log_handler)
    activity_log.setLevel(logging.INFO)
    act_log_handler.setFormatter(activity_log_formatter)
    return activity_log


def get_worker_log(analytics_name='Unknown'):
    worker_log = logging.getLogger("r5d4.worker.%s" % analytics_name)
    if settings.WORKER_LOG:
        worker_log_handler = RFHandler(settings.WORKER_LOG, "a+", 1048576, 15)
    else:
        worker_log_handler = logging.StreamHandler()
    worker_log.addHandler(worker_log_handler)
    worker_log.setLevel(settings.WORKER_LOG_LEVEL)
    worker_log_handler.setFormatter(worker_log_formatter)
    return worker_log

if __name__ == "__main__":
    al = get_activity_log()
    al.info("Testing Activity log")
    wl = get_worker_log()
    wl.info("Testing Worker log")
