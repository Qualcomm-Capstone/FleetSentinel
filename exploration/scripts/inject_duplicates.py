#!/usr/bin/env python3
"""이미 Kafka 에 있는 레코드를 **그대로 다시 넣는다** — dedup 검증용.

차량이 ack 을 못 받아 `committed+1` 부터 재전송하는 상황을 만든다. 헤더까지 동일하므로
`(vehicle_id, boot_id, seq)` 가 같고, Flink dedup 이 전부 막아야 한다.

사용:
    PYTHONPATH=. .venv/bin/python scripts/inject_duplicates.py --topic telemetry.signals --n 500
"""
from __future__ import annotations
import argparse
from confluent_kafka import Consumer, Producer, TopicPartition

ap = argparse.ArgumentParser()
ap.add_argument("--brokers", default="localhost:29092")
ap.add_argument("--topic", default="telemetry.signals")
ap.add_argument("--n", type=int, default=500)
ap.add_argument("--from-tail", action="store_true",
                help="토픽 끝에서 가져온다 — **현재 boot_id** 의 레코드라야 현실적인 "
                     "재전송 시나리오가 된다. 앞에서 가져오면 옛 boot_id 라 dedup 이 "
                     "BOOT_RESET 으로 통과시킨다")
args = ap.parse_args()

c = Consumer({"bootstrap.servers": args.brokers, "group.id": "dup-injector",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
md = c.list_topics(args.topic, timeout=10)
parts = list(md.topics[args.topic].partitions)
if args.from_tail:
    tps = []
    for p_ in parts:
        _, hi = c.get_watermark_offsets(TopicPartition(args.topic, p_), timeout=10)
        tps.append(TopicPartition(args.topic, p_, max(0, hi - args.n)))
    c.assign(tps)
else:
    c.assign([TopicPartition(args.topic, p, 0) for p in parts])

grabbed = []
while len(grabbed) < args.n:
    m = c.poll(10)
    if m is None:
        break
    if m.error():
        continue
    grabbed.append(m)
c.close()

p = Producer({"bootstrap.servers": args.brokers, "acks": "all",
              "enable.idempotence": True})
for m in grabbed:
    # 키·헤더·값을 전부 그대로. 게이트웨이가 쓴 것과 구분되지 않는다.
    p.produce(args.topic, key=m.key(), value=m.value(), headers=m.headers())
p.flush(30)
print(f"{args.topic} 에 {len(grabbed)}건 중복 주입 완료")
