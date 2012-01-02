#!/usr/bin/env python
from __future__ import absolute_import
import sys
from flask import json
from r5d4 import run


def publish(resource_json, resource_name, tr_type="insert"):
    client = run.app.test_client()
    for transaction in resource_json:
        rv = client.post('/resource/%s/' % (resource_name), data={
            "tr_type": tr_type,
            "payload": json.dumps(transaction)
        })

        if rv.status_code != 202:
            print "Unexpected status code: %d" % rv.status_code
            print "Response was: %s" % rv.data
        if rv.status_code >= 500:
            break


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        resource_data = json.load(open(sys.argv[1], 'r'))
        if len(sys.argv) >= 4:
            publish(resource_data, sys.argv[2], sys.argv[3])
        else:
            publish(resource_data, sys.argv[2])
    else:
        print "Usage: %s data.json resource_name [tr_type]" % sys.argv[0]
