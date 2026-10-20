#!/usr/bin/env python3
"""Real Kafka → Flink → API/SSE contract test, with explicitly synthetic monitor snapshots."""
import argparse
from collections import Counter
from io import BytesIO
import json
from pathlib import Path
import threading
import time
import urllib.request
import urllib.parse
import uuid
from confluent_kafka import Producer
from fastavro import schemaless_writer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--api-url', default='http://127.0.0.1:8080')
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    run = 'e2e-' + uuid.uuid4().hex[:8]
    schema = json.loads(Path('schemas/vehicle-signal.avsc').read_text())
    producer = Producer({'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094',
                         'acks': 'all', 'enable.idempotence': True, 'broker.address.family': 'v4'})
    events, failures = [], []
    ready = threading.Event()
    ready_count = 0
    ready_lock = threading.Lock()
    def listen(role):
        nonlocal ready_count
        try:
            vehicle = run + '-' + role
            query = urllib.parse.urlencode({'vehicle': vehicle})
            with urllib.request.urlopen(args.api_url + '/api/alerts?' + query, timeout=60) as response:
                with ready_lock:
                    ready_count += 1
                    if ready_count == 3:
                        ready.set()
                for line in response:
                    if line.startswith(b'data:'):
                        item = json.loads(line[5:])
                        events.append({'receivedAtMs': time.time_ns() // 1_000_000, **item})
        except Exception as error:
            failures.append(str(error))
    for role in ('stopped', 'planned', 'moving'):
        threading.Thread(target=listen, args=(role,), daemon=True).start()
    # Servlet headers may be committed by the first heartbeat rather than subscribe().
    ready.wait(5)
    start = time.time_ns() // 1_000_000
    seqs = Counter()
    sent = []
    def send(role, tick):
        vehicle = run + '-' + role
        seqs[vehicle] += 1
        seq = seqs[vehicle]
        speed = 0.0 if tick < 5 and role != 'moving' else 3.0
        health = 0.0 if role == 'moving' and tick in (1, 2) else 1.0
        rec = dict(scene_id='synthetic-monitor-contract', channel='fleet_status',
                   sensor_time=(start + tick * 1000) * 1000, log_time=(start + tick * 1000) * 1000,
                   values_num=dict(speed_mps=speed, mission_active=1.0,
                                   planned_stop=1.0 if role == 'planned' else 0.0, sensor_healthy=health),
                   values_vec={}, values_str=dict(mission_id='test-mission'))
        data = BytesIO(); schemaless_writer(data, schema, rec)
        headers = [('vehicle_id', vehicle), ('boot_id', run), ('seq', str(seq)), ('kind', 'RECORD_KIND_SIGNAL')]
        def delivered(error, message):
            if error: failures.append(str(error))
        producer.produce('telemetry.records', key=vehicle, value=data.getvalue(), headers=headers, on_delivery=delivered)
        if tick == 2:  # exact transport duplicate
            producer.produce('telemetry.records', key=vehicle, value=data.getvalue(), headers=headers, on_delivery=delivered)
        sent.append(dict(vehicle=vehicle, seq=seq, tick=tick))
    schedule = time.monotonic()
    for tick in range(6):
        time.sleep(max(0, schedule + tick - time.monotonic()))
        for role in ('stopped', 'planned', 'moving'): send(role, tick)
        if producer.flush(5): failures.append('producer flush incomplete')
    time.sleep(3)
    for role in ('stopped', 'planned', 'moving'): send(role, 9)
    producer.flush(5)
    deadline = time.monotonic() + 15
    while len(events) < 10 and time.monotonic() < deadline: time.sleep(.05)
    observed = Counter((e['vehicleId'].removeprefix(run + '-'), e['type']) for e in events)
    expected = Counter({('stopped', 'UNPLANNED_STOP'): 1, ('stopped', 'STOP_CLEARED'): 1,
                        ('moving', 'SENSOR_FAULT'): 1, ('moving', 'SENSOR_RECOVERED'): 1})
    for role in ('stopped', 'planned', 'moving'):
        expected[(role, 'TELEMETRY_STALE')] = 1
        expected[(role, 'TELEMETRY_RECOVERED')] = 1
    passed = observed == expected and not failures and len({e['eventId'] for e in events}) == len(events)
    report = dict(run=run, source='synthetic fleet_status; not nuScenes sensor faults',
                  path='Kafka → Flink → Kafka alerts → Spring API → SSE',
                  thresholdMs=dict(stop=3000, stale=2000), sent=sent, events=events,
                  expected=[dict(vehicle=k[0], type=k[1], count=v) for k,v in expected.items()],
                  failures=failures, passed=passed)
    (args.out / 'result.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(dict(run=run, passed=passed, events=len(events), failures=failures)))
    if not passed: raise SystemExit(1)


if __name__ == '__main__': main()
