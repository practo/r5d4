#!/usr/bin/env python
from __future__ import absolute_import
from flask import Flask, request, Response, json
from werkzeug.exceptions import BadRequest
import r5d4.settings as settings
from r5d4.analytics_browser import browse_analytics
from r5d4.publisher import publish_transaction
from r5d4.utility import json_response
from r5d4.logger import get_activity_log

app = Flask(__name__)

app.config["DEBUG"] = settings.DEBUG
app.config["REDIS_UNIX_SOCKET_PATH"] = settings.REDIS_UNIX_SOCKET_PATH
app.config["REDIS_HOST"] = settings.REDIS_HOST
app.config["REDIS_PORT"] = settings.REDIS_PORT
app.config["CONFIG_DB"] = settings.CONFIG_DB
app.config["DEFAULT_DATA_DB"] = settings.DEFAULT_DATA_DB
app.config["SECRET_KEY"] = settings.SECRET_KEY
activity_log = get_activity_log()


@app.errorhandler(404)
def not_found_handler(error):
    error_response = json.dumps({
        "status": "Not Found",
        "error_message": error.description[0],
        "error_context": error.description[1]
    }, indent=2)
    return Response(status=404, mimetype='application/json',
        response=error_response)


@app.errorhandler(400)
def bad_request_handler(error):
    error_response = json.dumps({
        "status": "Bad Request",
        "error_message": error.description[0],
        "error_context": error.description[1]
    }, indent=2)
    return Response(status=400, mimetype='application/json',
        response=error_response)


@app.errorhandler(503)
def service_unavailable_handler(error):
    error_response = json.dumps({
        "status": "Service Unavailable",
        "error_message": error.description[0],
        "error_context": error.description[1]
    }, indent=2)
    return Response(status=503, mimetype='application/json',
        response=error_response)


@app.route('/analytics/<analytics_name>/', methods=['GET'])
@json_response
def analytics(analytics_name):
    return browse_analytics(analytics_name, request.args)


@app.route('/resource/<resource>/', methods=['POST'])
def publish(resource):
    payload = request.form["payload"]
    tr_type = request.form["tr_type"]
    try:
        publish_transaction(resource, tr_type, payload)
        if activity_log:
            activity_log.info("%s\t%s\t%s", tr_type, resource, payload)
    except ValueError as e:
        raise BadRequest(e)
    return Response(status=202, mimetype='application/json',
                    response=json.dumps({"status": "Accepted"},
                                        indent=2))


if __name__ == "__main__":
    app.run()
