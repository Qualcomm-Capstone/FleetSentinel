"""Bounded MQTT request/reply for clip metadata; HTTPS carries only object parts.

The upload worker waits here; the MQTT owner keeps draining replies and telemetry.
Correlation IDs identify attempts. Durable file/job IDs in ClipUploader identify
operations across timeouts and restarts; broker PUBACK never completes an operation.
"""
import json
import threading
import uuid

from .clip_upload import HttpFailure, ObjectTransport
from .mqtt_contract import canonical, identity

MAX_CONTROL_BYTES = 1024 * 1024


class MqttClipTransport(ObjectTransport):
    def __init__(self, connection, vehicle_id, *, object_ca=None, timeout=15, **kwargs):
        super().__init__(object_ca=object_ca, **kwargs)
        self.connection = connection
        self.prefix = f'fleet/v1/{identity(vehicle_id)}/clip/'
        self.timeout = timeout
        self._condition = threading.Condition()
        self._pending = None
        self._response = None
        self._closed = False

    def control(self, method, path, body=None):
        # Internal adapter for the existing durable uploader, not an HTTP call.
        segments = path.strip('/').split('/') if path else []
        if method == 'POST' and not segments:
            operation, job_id = 'create', None
        elif method == 'GET' and len(segments) == 1:
            operation, job_id = 'status', segments[0]
        elif method == 'DELETE' and len(segments) == 1:
            operation, job_id = 'cancel', segments[0]
        elif method == 'POST' and len(segments) == 2 and segments[1] in ('part-urls', 'complete'):
            operation, job_id = segments[1].replace('-', '_'), segments[0]
        else:
            raise ValueError('unsupported clip operation')
        correlation = uuid.uuid4().hex
        request = dict(version=1, correlation_id=correlation, operation=operation,
                       job_id=job_id, body=body or {})
        wire = canonical(request)
        if len(wire) > MAX_CONTROL_BYTES:
            raise ValueError('clip request exceeds byte limit')
        with self._condition:
            if self._closed:
                raise ConnectionError('clip control closed')
            if self._pending is not None:
                raise RuntimeError('only one clip request may be in flight')
            self._pending, self._response = (correlation, operation), None
            try:
                self.connection.publish(self.prefix + 'request', wire)
                if not self._condition.wait_for(lambda: self._response is not None or self._closed, self.timeout):
                    raise TimeoutError('clip MQTT response timed out')
                if self._closed:
                    raise ConnectionError('clip control closed')
                response = self._response
                if not 200 <= response['status'] < 300:
                    raise HttpFailure(response['status'])
                return response['data']
            finally:
                self._pending = self._response = None

    def receive(self, topic, payload):
        if topic != self.prefix + 'response' or len(payload) > MAX_CONTROL_BYTES:
            return False
        try:
            value = json.loads(payload)
            if (not isinstance(value, dict) or type(value.get('version')) is not int or value['version'] != 1
                    or type(value.get('status')) is not int or not 200 <= value['status'] <= 599
                    or not isinstance(value.get('data'), dict)):
                return False
        except (ValueError, TypeError):
            return False
        with self._condition:
            if (self._pending is None or self._response is not None
                    or self._pending != (value.get('correlation_id'), value.get('operation'))):
                return False
            self._response = value
            self._condition.notify_all()
            return True

    def close(self):
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        super().close()
