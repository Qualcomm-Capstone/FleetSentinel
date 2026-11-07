"""MQTT upload control: correlated replies, bounded waiting, no transport ACK shortcut."""
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from fleetsentinel_ingest.clip_mqtt import MqttClipTransport, MAX_CONTROL_BYTES
from fleetsentinel_ingest.clip_upload import HttpFailure


class Connection:
    def __init__(self):
        self.sent = []
        self.ready = threading.Event()
    def publish(self, topic, payload):
        self.sent.append((topic, json.loads(payload)))
        self.ready.set()


def reply(transport, request, **changes):
    value = dict(version=1, correlation_id=request['correlation_id'], operation=request['operation'],
                 status=200, data={'state': 'UPLOADING'}) | changes
    return transport.receive('fleet/v1/vehicle-1/clip/response', json.dumps(value).encode())


def test_reply_requires_matching_topic_correlation_and_operation():
    connection = Connection()
    transport = MqttClipTransport(connection, 'vehicle-1', timeout=1)
    with ThreadPoolExecutor() as pool:
        result = pool.submit(transport.control, 'POST', '', {'request_id': 'durable-file'})
        assert connection.ready.wait(1)
        topic, request = connection.sent[0]
        assert topic == 'fleet/v1/vehicle-1/clip/request'
        assert request['operation'] == 'create'
        assert request['body'] == {'request_id': 'durable-file'}
        assert not reply(transport, request, correlation_id='stale')
        assert not reply(transport, request, operation='complete')
        assert not transport.receive('fleet/v1/vehicle-2/clip/response', json.dumps(request).encode())
        assert not result.done()  # publish/PUBACK is not a business response
        assert reply(transport, request)
        assert result.result(1) == {'state': 'UPLOADING'}
        assert not reply(transport, request)


def test_timeout_retries_with_new_correlation_but_same_durable_create_identity():
    connection = Connection()
    transport = MqttClipTransport(connection, 'vehicle-1', timeout=.01)
    for _ in range(2):
        with pytest.raises(TimeoutError):
            transport.control('POST', '', {'request_id': 'file-1'})
    first, second = [message for _, message in connection.sent]
    assert first['correlation_id'] != second['correlation_id']
    assert first['body'] == second['body']
    assert not reply(transport, first)


def test_shutdown_interrupts_wait_without_releasing_file():
    connection = Connection()
    transport = MqttClipTransport(connection, 'vehicle-1', timeout=30)
    with ThreadPoolExecutor() as pool:
        result = pool.submit(transport.control, 'GET', '/job-1')
        assert connection.ready.wait(1)
        transport.close()
        with pytest.raises(ConnectionError):
            result.result(1)


def test_error_response_preserves_retry_classification_and_bounds():
    connection = Connection()
    transport = MqttClipTransport(connection, 'vehicle-1', timeout=1)
    with ThreadPoolExecutor() as pool:
        result = pool.submit(transport.control, 'POST', '/job-1/complete', {'upload_id': 'old', 'parts': []})
        assert connection.ready.wait(1)
        request = connection.sent[0][1]
        assert request['job_id'] == 'job-1'
        assert reply(transport, request, status=409, data={})
        with pytest.raises(HttpFailure) as error:
            result.result(1)
        assert error.value.status == 409
    with pytest.raises(ValueError, match='limit'):
        transport.control('POST', '', {'padding': 'x' * MAX_CONTROL_BYTES})
    assert len(connection.sent) == 1
