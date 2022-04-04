import pytest
from app import app

from cloudevents.http import CloudEvent, to_binary, from_http


@pytest.fixture
def client():
    app.testing = True
    return app.test_client()


def test_missing_subject(client):
    attributes = {
        "type": "io.drogue.event.v1",
        "source": "https://example.com/event-producer",
    }
    data = {"message": "Hello World!"}

    event = CloudEvent(attributes, data)
    headers, body = to_binary(event)

    r = client.post("/", headers=headers, data=body)
    assert r.status_code == 400


def test_unknown_data(client):
    attributes = {
        "type": "io.drogue.event.v1",
        "source": "https://example.com/event-producer",
    }
    data = "some data"

    event = CloudEvent(attributes, data)
    headers, body = to_binary(event)

    r = client.post("/", headers=headers, data=body)
    assert r.status_code == 400


def test_unknown_event(client):
    # This data defines a binary cloudevent
    attributes = {
        "type": "io.drogue.event.v1",
        "source": "https://example.com/event-producer",
        "subject": "foo/bar"
    }
    data = {"_timestamp": 1648802831, "foo": "bar"}

    event = CloudEvent(attributes, data)
    headers, body = to_binary(event)

    r = client.post("/", headers=headers, data=body)
    assert r.status_code == 200

    event = from_http(r.headers, r.data)
    assert event.data == {"_timestamp": 1648802831, "foo": "bar"}
    assert event['type'] == "io.drogue.event.v1"

def test_binary_request(client):
    # This data defines a binary cloudevent
    attributes = {
        "type": "io.drogue.event.v1",
        "source": "https://example.com/event-producer",
        "subject": "temperature/tool0"
    }
    data = {"_timestamp": 1648802831, "actual": 24.92, "target": 0.0}

    event = CloudEvent(attributes, data)
    headers, body = to_binary(event)

    r = client.post("/", headers=headers, data=body)
    assert r.status_code == 200

    event = from_http(r.headers, r.data)
    assert event.data["features"]["tool0"]["temperature"] == {
        "timestamp": 1648802831000,
        "actual": 24.92,
        "target": 0.0
    }
    assert event['type'] == "org.octoprint.temperature.v1"
