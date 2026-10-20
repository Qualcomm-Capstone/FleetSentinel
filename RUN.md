# FleetSentinel — 로컬 인프라 실행 절차

> Kafka에서 독립적으로 읽는 ClickHouse 적재 Job과 알림 Job을 구현했다.
> 두 Job은 consumer group과 checkpoint를 공유하지 않는다.
> 실제 차량 대신 nuScenes 재생기를 사용하므로 실차량 라이브 관제는 아니다.
>
> 데이터 설계는 [`docs/data-design.md`](docs/data-design.md), 전체 설계는
> [`docs/sdd.md`](docs/sdd.md)를 참고한다.

## 0. 사전 준비

- Docker Desktop (메모리 6GB 이상 권장 — Kafka 3 + Flink 2 + ClickHouse + MinIO)
- `make`, `bash`

## 1. 스택 기동

```bash
make up      # 전 서비스 healthy까지 대기
make ps      # 상태 확인
```

기동되는 서비스:

| 서비스 | 포트 | 용도 |
|---|---|---|
| kafka1 / kafka2 / kafka3 | 29092 (호스트) | KRaft 3-broker, RF=3 / min.insync.replicas=2 |
| jobmanager / taskmanager | 8081 | Flink 클러스터 |
| minio | 9000 / 9001 | S3 호환 오브젝트 스토리지 (GCS 로컬 대체) |
| clickhouse | **8124**(HTTP) / 9009(네이티브) | 신호·객체 메타데이터 시계열, 클립 카탈로그 |

## 2. 토픽 부트스트랩

```bash
make topics
```

`telemetry.records`, `telemetry.dlq`, `fleet.alerts`를 RF=3 / `min.insync.replicas=2`로 생성한다.
`fleet.alerts`는 저빈도 전이 이벤트의 전체 순서와 SSE 재개를 단순하게 유지하기 위해 1파티션이다.

## 3. 스모크 테스트

```bash
make smoke
```

전 서비스 healthy · 토픽 존재 · **ClickHouse 질의·지리 함수** · MinIO 버킷을 단언한다.

## 4. Kafka HA broker-kill 데모

```bash
make ha-demo
```

전용 토픽(`ha-demo`)에 `kafka-producer-perf-test`로 발행하는 도중 브로커 1대를 **하드 kill(SIGKILL)**
하고, 리더 재선출 → ISR 복원 → **유실 0**(consumed ≥ published)을 오프셋으로 대사한다.

- 외부 의존이 없다 — 부하 발생기는 Kafka 이미지 내장 도구를 쓴다.
- `VICTIM=kafka3 make ha-demo` 처럼 대상 브로커를 바꿀 수 있다.
- **정직한 한계(ADR-009)**: 단일 호스트 3-broker이므로 broker-level 복제·failover만 실증한다.
  호스트·존 SPOF는 스코프 밖이다.

## 5. 종료

```bash
make down     # 볼륨 유지
make clean    # 볼륨·데이터까지 삭제
```

## 5.1 Flink와 실시간 알림 실행

```bash
make ch-schema
make flink-test
make flink-submit
make api
```

`make flink-submit`의 `ODD_BOUNDS` 기본값은 fixture에서 확인한 보스턴·싱가포르 지도 범위를
감싼 사각형이다. 경계 전이 테스트처럼 더 좁은 범위를 쓰려면 실행할 때 덮어쓴다.

```bash
make flink-submit ODD_BOUNDS='singapore-onenorth=1.2983,1.2987,103.7883,103.7885;boston-seaport=42.3445,42.3518,-71.0507,-71.0341'
```

제출 뒤 Flink UI에는 두 Job이 보여야 한다.

- `fleetsentinel-alerts`: `fleetsentinel-alerts` consumer group으로 읽고 `fleet.alerts`에 기록한다.
- `fleetsentinel-storage`: `fleetsentinel-storage` consumer group으로 읽고 ClickHouse에 적재한다.

ClickHouse가 멈추면 storage Job의 lag와 checkpoint만 영향을 받아야 한다. alert Job 실행 그래프에는
ClickHouse sink가 없으므로 저장 장애의 backpressure가 알림 판정까지 전파되지 않는다.
다만 두 Job은 같은 세션 클러스터의 CPU·메모리와 Kafka를 공유하므로 호스트 장애나 자원 고갈까지
격리되는 것은 아니다. 기존 단일 Job을 사용했다면 먼저 중지한 뒤 두 Job을 제출한다.
새 consumer group은 보존된 로그의 처음부터 읽는다. 재배포 시 연산자 상태까지 이어가려면
각 Job의 checkpoint/savepoint에서 별도로 복원해야 하며, Kafka 커밋 offset만으로는 상태가 복원되지 않는다.

대시보드를 실 API에 연결한다.

```bash
make dashboard
```

알림 경로는 `Flink alert Job → fleet.alerts → Spring API → /api/alerts SSE → React`다.
`/api/stream`은 ClickHouse의 텔레메트리 재생 전용이라 알림 전달에 사용하지 않는다.

## 트러블슈팅

| 증상 | 원인 / 조치 |
|---|---|
| `make up`이 healthy 대기에서 멈춤 | Docker 메모리 부족. Desktop 설정에서 8GB 이상 할당 |
| ClickHouse HTTP 응답이 이상함 | **호스트 8123을 다른 프로세스가 점유**할 수 있어 8124로 매핑했다. `lsof -nP -iTCP:8124 -sTCP:LISTEN`으로 확인 |
| Flink TaskManager exit 137 | OOM. Docker 메모리 상향 후 `make restart` |
| `ha-demo` 중 ISR 미복원 | 브로커 재기동이 느린 경우. `make ps`로 상태 확인 후 재실행 |

## 6. 차량 측 구현 테스트 (인프라 불필요)

온보드 유실 방지 경로 — WAL · 누적 ack · `seq` dedup. 도커 스택 없이 돈다.

```bash
cd exploration
./setup-venv.sh                                    # 최초 1회 (nuScenes devkit numpy 충돌 회피 2단계)
PYTHONPATH=. .venv/bin/python -m pytest tests/ -q  # 82건
```

SIGKILL 테스트가 자식 프로세스를 강제 종료하므로 몇 초 걸린다. 설계는
[`docs/wal-design.md`](docs/wal-design.md) · [`docs/ack-dedup-design.md`](docs/ack-dedup-design.md).

## 7. 대시보드 (인프라 불필요)

```bash
cd frontend
npm install
npm run dev        # Vite + 목업 SSE 스트림
```

**목업 스트림은 난수가 아니다** — 실 nuScenes에서 뽑은 픽스처를 설계상 전송 단위로 재생한다.
픽스처는 라이선스상 커밋하지 않으므로 없으면 생성해야 한다
([`frontend/README.md`](frontend/README.md)).

## 다음 단계

`scripts/verify-realtime.py`, `scripts/flink_validation/run_campaign.py`,
`scripts/verify-recovery.py`로
알림 전달, 고정 유입 처리, TaskManager 복구를 검증할 수 있다. 실행 결과는 로컬의
`evidence/` 아래에 저장하며 저장소에는 올리지 않는다.

Flink 정상/ClickHouse 10초 장애 캠페인은 검증 전용 Prometheus를 함께 실행한다.
`data/nuscenes`의 mini 데이터·CAN bus 데이터와 `exploration/setup-venv.sh`로 설치한 의존성이 필요하다.
기본 API 주소는 `http://127.0.0.1:8080`이며 다른 포트는 `--api-url`로 지정한다.
이 테스트는 Kafka에 레코드를 쓰고 ClickHouse를 일시 정지하므로 전용 로컬 개발 스택에서만 실행한다.
실행 전 두 Job의 checkpoint가 각각 5회 이상 완료되고 consumer lag가 0이어야 한다.

```bash
docker compose -f infra/docker-compose.yml --profile validation up -d --wait prometheus-flink
cd exploration
PYTHONPATH=. .venv/bin/python ../scripts/flink_validation/run_campaign.py \
  --out ../evidence/flink-split-$(date +%Y%m%d-%H%M%S) --metrics-bind 0.0.0.0
```

각 스크립트에 `--out evidence/<새 실행 이름>`을 지정한다. 알림 시나리오 검증은
Flink 잡을 `--stop-after-ms 3000 --monitor-stale-ms 2000`으로 제출해야 한다.
복구 검증은 로컬 `fleet-taskmanager` 컨테이너를 실제 재시작하므로 개발 환경에서만 실행한다.
