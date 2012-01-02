from __future__ import absolute_import
from werkzeug.exceptions import ServiceUnavailable, NotFound
from r5d4.flask_redis import get_conf_db


def publish_transaction(channel, tr_type, payload):
    conf_db = get_conf_db()
    if tr_type not in ["insert", "delete"]:
        raise ValueError("Unknown transaction type", tr_type)
    subscribed = conf_db.scard("Subscriptions:%s:ActiveAnalytics" % channel)
    if subscribed == 0:
        raise NotFound(("Channel not found",
                "Channel '%(channel)s' is not found or has 0 subscriptions" %
                locals()))
    listened = conf_db.publish(channel,
        '{'
        '  "tr_type" : "' + tr_type + '", '
        '  "payload" : ' + payload +
        '}')
    if listened != subscribed:
        raise ServiceUnavailable(("Subscription-Listened mismatch",
                "Listened count = %d doesn't match Subscribed count = %d" % (
                        listened, subscribed)))
