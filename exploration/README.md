# 차량 수집 구현과 데이터 도구

| 경로 | 역할 |
|---|---|
| `fleetsentinel_ingest/collection_policy.py` | 상황·신호별 수집 정책과 명령 적용 |
| `fleetsentinel_ingest/telemetry_store.py` | SQLite ring·outbox·배치·명령 결과 보존 |
| `fleetsentinel_ingest/mqtt_contract.py`, `mqtt_bridge.py`, `mqtt_runtime.py` | MQTT 전송 계약, Kafka 적재, 재전송 루프 |
| `fleetsentinel_ingest/command_service.py` | 관제 명령·적용 결과 관리 |
| `scripts/mqtt_ingest.py` | 차량 JSONL 입력·bridge·관제 명령 진입점 |
| `fleetsentinel_ingest/wal.py`, `shipping.py`, `grpc_transport.py` | 기존 gRPC 수집·재전송 호환 경로 |
| `scripts/`의 데이터 변환·측정 도구 | nuScenes 분석, MCAP 변환, 재생·부하 생성 |
| `tests/` | 저장·전송·수집 정책 및 데이터 변환 회귀 검증 |

설계는 [SDD](../docs/sdd.md), MQTT 입력·ACK·명령은 [수집 계약](../docs/mqtt-contract.md),
데이터 형식과 측정 조건은 [데이터 설계](../docs/data-design.md)를 따른다.

## 의존성과 검증

`requirements.in`에 직접 의존성을 관리한다.
nuScenes devkit의 NumPy 상한과 Rerun의 NumPy 2 요구가 충돌해
`setup-venv.sh`에서 devkit 설치 후 NumPy를 교체한다. 일반적인 단일 pip 설치로 대체하지 않는다.
이 조합의 데이터 변환·좌표 검증은 해당 테스트로 확인해야 한다.

저장소 루트에서 `make mqtt-test`와 `make mqtt-integration-test`를 사용한다.
통합 테스트에는 Docker와 OpenSSL이 필요하며 임시 인증서·컨테이너를 생성해 종료 후 정리한다.
