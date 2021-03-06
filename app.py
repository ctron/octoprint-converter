import os

from flask import Flask, request, make_response
from cloudevents.exceptions import *
from cloudevents.http import from_http, to_binary

app = Flask(__name__)

#
# device state:
#
# {
#   "features": {
#      "firmware": {
#         "timestamp: 0,
#         "name": "",
#      }
#      "tool0": {
#         "temperature": {
#            "timestamp": 0,
#            "actual": 23.0,
#            "target": 200.0,
#         }
#      }
#   }
# }
#


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

    if channel.startswith("progress/printing"):
        return "org.octoprint.printing.progress.v1", convert_printing_progress(data)

    if channel.startswith("event/"):
        new_data = convert_event(str.removeprefix(channel, "event/"), data)
        if new_data:
            return "org.octoprint.printer.connection.v1", new_data

    return None, data


def feature(name: str, properties: dict):
    return {
        "features": {
            name: properties
        }
    }


def convert_event(name: str, data):
    timestamp = data["_timestamp"] * 1000
    if name == "PrinterStateChanged":
        return printer_state(timestamp, data)
    if name == "FirmwareData":
        return printer_firmware(timestamp, data)

    return None


def printer_firmware(timestamp, data):
    return feature("firmware", {
        "timestamp": timestamp,
        "name": data["name"],
        "data": data["data"],
    })


def printer_state(timestamp, data):
    return feature("connection", {
        "timestamp": timestamp,
        "state": data["state_string"],
        "state_id": data["state_id"],
        "connected": data["state_id"] == "OPERATIONAL",
    })


def convert_temperature(tool: str, data):
    return feature(tool, {
        "temperature": {
            "timestamp": data['_timestamp'] * 1000,
            "actual": data['actual'],
            "target": data['target']
        }
    })


def convert_printing_progress(data):
    return feature("printing", {
        "timestamp": data['_timestamp'] * 1000,
        "progress": data['progress'],
        "location": data['location'],
        "path": data['path'],
    })


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
