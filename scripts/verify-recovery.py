#!/usr/bin/env python3
"""Restart only fleet-taskmanager; verify real checkpoint recovery and sensor transition state."""
from io import BytesIO
import argparse
import json
from pathlib import Path
import subprocess
import threading
import time
import urllib.request
import urllib.parse
import uuid
from confluent_kafka import Producer
from fastavro import schemaless_writer


def get(path): return json.load(urllib.request.urlopen('http://localhost:8081'+path, timeout=10))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--api-url',default='http://127.0.0.1:8080')
    args=parser.parse_args()
    out=args.out
    out.mkdir(parents=True,exist_ok=False)
    jobs = [j['jid'] for j in get('/jobs/overview')['jobs']
            if j['state'] == 'RUNNING' and j['name'] == 'fleetsentinel-alerts']
    if len(jobs) != 1:
        raise RuntimeError(f'expected exactly one running alert job, got {jobs}')
    job = jobs[0]
    run='recovery-'+uuid.uuid4().hex[:8]
    schema=json.loads(Path('schemas/vehicle-signal.avsc').read_text())
    producer=Producer({'bootstrap.servers':'localhost:29092','acks':'all','broker.address.family':'v4'})
    events=[]
    def listen():
        query = urllib.parse.urlencode({'vehicle': run})
        with urllib.request.urlopen(args.api_url+'/api/alerts?'+query,timeout=180) as response:
            for line in response:
                if line.startswith(b'data:'):
                    e=json.loads(line[5:])
                    if e['vehicleId']==run: events.append(e)
    threading.Thread(target=listen,daemon=True).start()
    def send(seq,healthy):
        ts=time.time_ns()//1000
        rec=dict(scene_id='synthetic-recovery',channel='fleet_status',sensor_time=ts,log_time=ts,
                 values_num=dict(speed_mps=3.0,mission_active=1.0,planned_stop=0.0,sensor_healthy=healthy),
                 values_vec={},values_str=dict(mission_id='recovery-mission'))
        b=BytesIO();schemaless_writer(b,schema,rec)
        producer.produce('telemetry.records',key=run,value=b.getvalue(),headers=[('vehicle_id',run),
                         ('boot_id',run),('seq',str(seq)),('kind','RECORD_KIND_SIGNAL')])
        assert producer.flush(10)==0
    send(1,0.0)
    deadline=time.monotonic()+30
    while not any(e['type']=='SENSOR_FAULT' for e in events) and time.monotonic()<deadline: time.sleep(.2)
    assert any(e['type']=='SENSOR_FAULT' for e in events), 'initial event absent'
    after=time.time_ns()//1_000_000
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        before=get(f'/jobs/{job}/checkpoints')
        if before['latest']['completed']['trigger_timestamp']>after:break
        time.sleep(.5)
    else:raise RuntimeError('checkpoint after initial state absent')
    began=time.time_ns()//1_000_000
    subprocess.run(['docker','compose','-f','infra/docker-compose.yml','restart','taskmanager'],
                   check=True,stdout=subprocess.PIPE)
    history=[];deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        current=get(f'/jobs/{job}/checkpoints')
        state=get(f'/jobs/{job}')['state']
        history.append(dict(atMs=time.time_ns()//1_000_000,state=state,restored=current['counts']['restored']))
        if state=='RUNNING' and current['counts']['restored']>before['counts']['restored']:break
        time.sleep(1)
    else:raise RuntimeError('checkpoint recovery not observed')
    recovered=time.time_ns()//1_000_000
    send(2,0.0);time.sleep(1);send(3,1.0)
    deadline=time.monotonic()+5
    while not any(e['type']=='SENSOR_RECOVERED' for e in events) and time.monotonic()<deadline:time.sleep(.1)
    sensor=[e for e in events if e['type'] in ('SENSOR_FAULT','SENSOR_RECOVERED')]
    passed=[e['type'] for e in sensor]==['SENSOR_FAULT','SENSOR_RECOVERED']
    result=dict(run=run,job=job,passed=passed,recoveryMs=recovered-began,before=before,
                after=current,history=history,events=events,
                scope='local TaskManager process restart, not host/zone failure; sensor state retained')
    (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(dict(passed=passed,recoveryMs=recovered-began,sensorEvents=len(sensor))))
    if not passed:raise SystemExit(1)

if __name__=='__main__':main()
