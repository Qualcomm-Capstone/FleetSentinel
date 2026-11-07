"""Real broker mTLS/ACL and edge reply handling (opt-in Docker gate)."""
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pytest

from .test_mqtt_integration import ROOT, broker, connect, until, pytestmark
from fleetsentinel_ingest.clip_mqtt import MqttClipTransport


def test_clip_control_large_metadata_reply_loss_and_topic_acl(broker):
    server = connect(broker, 'clip-service', ['$share/clip-control/fleet/v1/+/clip/request'])
    edge = connect(broker, 'vehicle-1', ['fleet/v1/vehicle-1/clip/response'])
    other = connect(broker, 'vehicle-2', ['fleet/v1/vehicle-2/clip/response'])
    transport = MqttClipTransport(edge, 'vehicle-1', timeout=.4)
    try:
        requests = []
        body = dict(request_id='durable-file', calibration={'camera': 'x' * 70000})
        with ThreadPoolExecutor() as pool:
            first = pool.submit(transport.control, 'POST', '', body)
            until(lambda: (server.drain(lambda t, p: requests.append(json.loads(p))), bool(requests))[1])
            with pytest.raises(TimeoutError):
                first.result(2)  # no application reply, even though broker accepted it
            transport.timeout = 3
            second = pool.submit(transport.control, 'POST', '', body)
            until(lambda: (server.drain(lambda t, p: requests.append(json.loads(p))), len(requests) == 2)[1])
            assert requests[0]['body'] == requests[1]['body']
            for index in (0, 1):
                request = requests[index]
                result = dict(version=1, correlation_id=request['correlation_id'], operation='create',
                              status=200, data={'id': 'job-1', 'padding': 'x' * 70000})
                server.publish('fleet/v1/vehicle-1/clip/response', json.dumps(result).encode())
                until(lambda: not edge.messages.empty())
                edge.drain(transport.receive)
                if index == 0:
                    assert not second.done()  # late reply to timed-out attempt
            assert second.result(2)['id'] == 'job-1'
        denied = []
        edge.client.on_publish = lambda c, u, mid, reason, props: denied.append(reason)
        edge.publish('fleet/v1/vehicle-2/clip/request', b'{}')
        edge.publish('fleet/v1/vehicle-1/clip/response', b'{}')
        until(lambda: len(denied) == 2)
        assert all(reason.is_failure for reason in denied)
        assert other.messages.empty()
    finally:
        transport.close()
        edge.close()
        other.close()
        server.close()


def test_replayed_clip_reference_collapses_in_actual_clickhouse_catalog_view():
    schema = (ROOT / 'infra/clickhouse/001-schema.sql').read_text()
    table = re.search(r'CREATE TABLE IF NOT EXISTS fleet\.segments_raw\b.*?;', schema, re.S)[0]
    view = re.search(r'CREATE VIEW IF NOT EXISTS fleet\.segments\b.*?;', schema, re.S)[0]
    sql = 'CREATE DATABASE fleet;\n' + table + '\n' + view + """
        INSERT INTO fleet.segments_raw
          (vehicle_id,boot_id,seq,segment_id,blob_uri,t_start,t_end,ingest_time,
           sensor_channels,size_bytes,checksum,sample_count,state,calibration)
        VALUES
          ('v1','clip-one',0,'one','s3://bucket/one','2026-11-01','2026-11-01',
           '2026-11-01', ['camera'],3,repeat('a',64),1,'UPLOADED',{}),
          ('v1','clip-one',0,'one','s3://bucket/one','2026-11-01','2026-11-01',
           '2026-11-07', ['camera'],3,repeat('a',64),1,'UPLOADED',{}),
          ('v1','clip-two',0,'two','s3://bucket/two','2026-11-01','2026-11-01',
           '2026-11-07', ['camera'],3,repeat('b',64),1,'UPLOADED',{});
        SELECT segment_id,blob_uri FROM fleet.segments ORDER BY segment_id FORMAT JSONEachRow;
    """
    result = subprocess.run(['docker', 'run', '--rm', '-i', '--network', 'none', '--entrypoint',
                             'clickhouse', 'clickhouse:26.3', 'local', '--multiquery'],
                            input=sql, text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        {'segment_id': 'one', 'blob_uri': 's3://bucket/one'},
        {'segment_id': 'two', 'blob_uri': 's3://bucket/two'},
    ]
