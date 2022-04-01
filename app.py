import os

from flask import Flask, request, Response, make_response
from cloudevents.exceptions import *
from cloudevents.http import from_http, to_binary

app = Flask(__name__)


class ConversionException(Exception):
    pass


def process(event):
    if event['type'] != 'io.drogue.event.v1':
        # we don't know the type -> ignore it
        return event

    channel = event['subject']
    if not channel:
        # channel is missing -> client error
        raise ConversionException("Missing channel")

    if type(event.data) is not dict:
        raise ConversionException(f"Unknown data type: {type(event.data)}")

    ty, event.data = convert(channel, event.data)

    if ty:
        event['type'] = ty
    # print(data)

    return event


def convert(channel: str, data):
    if channel.startswith("temperature/"):
        return "org.octoprint.temperature.v1", convert_temperature(str.removeprefix(channel, "temperature/"), data)

    return None, data


def convert_temperature(tool: str, data):
    return {
        tool: {
            "temperature": {
                "timestamp": data['_timestamp'],
                "actual": data['actual'],
                "target": data['target']
            }
        }
    }


@app.route('/', methods=["POST"])
def index():
    # parse event

    try:
        event = from_http(request.headers, request.get_data())
    except (InvalidStructuredJSON, MissingRequiredFields) as err:
        return f"Invalid data: {err}", 400

    # process

    try:
        event = process(event)
    except (ConversionException, KeyError) as err:
        return f"Failed to process: {err}", 400

    # convert to response

    headers, payload = to_binary(event)
    response = make_response(payload, 200)
    response.headers = headers

    # done

    return response


if __name__ == "__main__":
    app.run(
        debug=True,
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 8080)))
