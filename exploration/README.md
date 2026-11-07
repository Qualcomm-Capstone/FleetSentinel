# 차량 수집 구현과 데이터 도구

| 경로 | 역할 |
|---|---|
| `fleetsentinel_ingest/collection_policy.py` | 상황·신호별 수집 정책과 명령 적용 |
| `fleetsentinel_ingest/telemetry_store.py` | SQLite ring·outbox·배치·명령 결과 보존 |
| `fleetsentinel_ingest/mqtt_contract.py`, `mqtt_bridge.py`, `mqtt_runtime.py` | MQTT 전송 계약, Kafka 적재, 재전송 루프 |
| `fleetsentinel_ingest/command_service.py` | 관제 명령·적용 결과 관리 |
| `fleetsentinel_ingest/raw_store.py` | 관리 원본 spool, 사건·요청 선택, 파일 pin·완료 receipt |
| `fleetsentinel_ingest/raw_recorder.py` | MCAP 회전·복구·원천 파일 읽기 전용 재생 |
| `fleetsentinel_ingest/clip_mqtt.py` | MQTT 요청·응답 연결, 시간 초과·늦은 응답 처리 |
| `fleetsentinel_ingest/clip_upload.py` | HTTPS multipart 전송, 영속 재시도·최종 완료 확인 |
| `scripts/mqtt_ingest.py` | 차량 JSONL 입력·bridge·관제 명령 진입점 |
| `fleetsentinel_ingest/wal.py`, `shipping.py`, `grpc_transport.py` | 기존 gRPC 수집·재전송 호환 경로 |
| `scripts/`의 데이터 변환·측정 도구 | nuScenes 분석, MCAP 변환, 재생·부하 생성 |
| `tests/` | 저장·전송·수집 정책 및 데이터 변환 회귀 검증 |

설계는 [SDD](../docs/sdd.md), MQTT 입력·ACK·명령은 [수집 계약](../docs/mqtt-contract.md),
데이터 형식과 측정 조건은 [데이터 설계](../docs/data-design.md)를 따른다.

## 원본 클립 수명주기

차량 이상 입력은 사건 10초 전·20초 후, 운영자 `clip_request`는 최대 60초 구간을 선택한다.
요청·선택 파일 pin·명령 처리 결과는 신호 outbox와 같은 SQLite DB에 저장한다. `applied`는 요청의
영속 수용을 뜻하며, 새 command ID의 `clip_status`로 확보·누락 구간과 파일별 전송 상태를 조회한다.

비선택 원본은 기본 1시간·128 GiB로 제한하고 MCAP은 10초·256 MiB를 기준으로 회전한다.
선택 파일은 16 MiB part·4 MiB/s·worker 1개로 전송한다. 서버가 객체 검증과 참조 Kafka ACK를 기록한
`COMPLETED` receipt를 로컬에 저장해야 pin을 해제한다. 선택된 미확인 파일은 자동 만료하지 않으며
용량이 부족하면 새 원본 수용에 실패한다. 파일 여러 개를 공유하는 요청은 중복 업로드를 재사용한다.

삭제 대상은 전용 관리 spool에 등록된 파일이다. nuScenes 등 기존 데이터 디렉터리를 spool로 지정하지 않는다.
센서 입력·원본 기록·MQTT 전송·HTTPS 업로드는 구분해 실행한다. 원본 제어와 메타데이터 전달은 기존 MQTT 연결을 사용한다.
객체 전송은 HTTPS이며 차량 인증서를 저장소에 보내지 않는다.
실제 센서 드라이버와 Flink 경보의 자동 원본 요청은 현재 범위에 포함하지 않는다.

## 실행

로컬 Kafka·MQTT 브로커·bridge와 MQTT 인증서를 준비한 환경을 전제로 한다. 저장소 루트에서
`bash scripts/gen-certs.sh`로 기존 gRPC/객체 HTTPS 인증서를 준비하고 다음 서비스를 실행한다.

```sh
docker compose --env-file /dev/null -f infra/docker-compose.yml --profile clips up -d postgres minio-init minio-tls
make gateway MQTT_PKI=./mqtt-pki
```

`make gateway`는 외부 presign 주소 `https://localhost:9443`을 사용한다. 서버의 MinIO 내부 접속은
별도 endpoint를 사용하며 서명된 URL의 host·경로는 TLS proxy가 유지한다. 배포 시에는 실제 외부 HTTPS
주소와 TLS 종단을 준비해야 한다. Kubernetes의 endpoint 예시만으로 운영 ingress가 생성되지는 않는다.

서버 MQTT 설정·CN별 인증서·다중 인스턴스 조건은 [MQTT 수집 계약](../docs/mqtt-contract.md#원본-업로드-제어와-참조-메타데이터)을 따른다.
새 인증서 디렉터리는 `bash scripts/gen-mqtt-certs.sh ./mqtt-pki vehicle-0001`로 생성한다. 기존 디렉터리를 덮어쓰지 않는다.

아래는 저장소 루트에서 실행하는 차량 예시다. `./mqtt-pki`는 별도로 준비한 MQTT 인증서 디렉터리이며,
`--client-id`는 인증서 CN과 맞춘다. 클립 제어도 같은 MQTT 차량 인증서를 사용한다. 서버에는 별도의 `clip-service` 인증서를 제공한다.

```sh
PYTHONPATH=exploration exploration/.venv/bin/python exploration/scripts/mqtt_ingest.py edge \
  --host localhost --port 8883 --vehicle vehicle-0001 --client-id vehicle-0001 \
  --ca ./mqtt-pki/ca.crt --cert ./mqtt-pki/vehicle-0001.crt --key ./mqtt-pki/vehicle-0001.key \
  --db ./data/edge.sqlite --raw-spool ./data/raw-spool \
  --raw-mcap ./data/mcap/scene-0061.mcap \
  --object-ca ./pki/ca/ca.crt --legacy-wal ./wal-segments
```

입력 MCAP 경로와 기존 WAL 경로는 실제 위치로 바꾼다. `--raw-mcap`은 여러 번 지정할 수 있으며,
생략하면 보존 중인 spool의 업로드만 재개한다. 재생은 첫 원본 시각을 현재 차량 시각으로 옮겨 진행하고
원천 파일은 변경하지 않는다. 신호 JSONL stdin 입력은 기존 방식대로 사용할 수 있다.
이상 입력의 `sensor_time`은 재생 원본과 같은 시각 축으로 맞춘다. 원본 구간은 입력을 처리한 시각이
아닌 센서 발생 시각으로 선택한다.
`--object-ca`는 객체 서버의 추가 신뢰 CA이며 기본 시스템 신뢰 저장소와 함께 사용한다.

인증서·설정 문제를 수정한 뒤 로컬 `FAILED` 작업을 재시도하려면 재시작 시 `--retry-failed-clips`를
명시한다. 기존 manifest·서버 작업 ID·part ETag를 유지한다. 서버가 확정한 `FAILED`는 그대로 남고
파일 pin도 유지하므로 체크섬 불일치 등 서버 오류는 별도 조사해야 한다.
장기 단절로 multipart가 만료되면 서버는 같은 작업에 새 `upload_id`를 발급한다. 차량은 새 세대를
조회하고 기존 ETag를 비운 뒤 part를 다시 전송하며, 이전 세대의 complete는 409로 거절된다.

`--legacy-wal` 검사에서 미확인 업로드·참조가 발견되면 시작을 거절한다. 기존 버전으로 drain하고
다시 실행한다. 새 버전에서는 `ship_segments`와 gRPC SegmentUpload를 제공하지 않는다.
관제 명령은 기존 `operator --command <JSON 파일>`로 제출하고 reported 결과를 확인한다.
원본 요청 JSON·시각 단위는 [MQTT 수집 계약](../docs/mqtt-contract.md#3-입력명령-계약),
인프라·전환 조건은 [SDD](../docs/sdd.md#5-rollout--rollback)를 따른다.

## 의존성과 검증

`requirements.in`에 직접 의존성을 관리한다.
nuScenes devkit의 NumPy 상한과 Rerun의 NumPy 2 요구가 충돌해
`setup-venv.sh`에서 devkit 설치 후 NumPy를 교체한다. 일반적인 단일 pip 설치로 대체하지 않는다.
이 조합의 데이터 변환·좌표 검증은 해당 테스트로 확인해야 한다.

저장소 루트에서 `make mqtt-test`와 `make mqtt-integration-test`를 사용한다.
통합 테스트에는 Docker와 OpenSSL이 필요하며 임시 인증서·컨테이너를 생성해 종료 후 정리한다.

MQTT 업로드는 v2 Avro 바이너리 배치로 전송하며 센서 payload를 base64로 변환하지 않는다.
ACK·명령·reported와 stdin 입력은 JSON을 유지한다. 새 bridge는 v1·v2를 모두 수신하고, 차량은
기존 SQLite의 v1 배치를 원래 ID로 재전송한 뒤 새 배치부터 v2를 사용한다. bridge를 먼저 배포하고
차량을 갱신한다. [와이어 계약과 롤백 조건](../docs/mqtt-contract.md#mqtt-업로드-바이너리-형식과-호환성)을 따른다.

128 GiB 상한은 원본 payload 기준이다. SQLite 본체는 메타데이터·인덱스를 포함해 기본 2 GiB이며
`--max-db-bytes`로 조정한다. 완료 요청·receipt·삭제 파일 기록은 유지하므로 장기 운행 시
`diagnostic.storage`의 DB 할당량·재사용 가능 공간·WAL 크기·파일시스템 여유 공간을 확인한다.
WAL은 1,000페이지마다 자동 체크포인트를 시도하고 재사용 시 파일 크기를 8 MiB로 제한한다.
활성 WAL과 원본 파일에 필요한 공간은 DB 본체 상한과 별도로 확보한다.

일반 신호는 큐에 도착한 최대 32건을 한 트랜잭션으로 저장하며 `synchronous=FULL`을 유지한다.
배치를 채우기 위해 기다리지 않고, 이상 이벤트는 별도로 커밋한다. 저장 실패 시 미커밋 입력을
유지하고 역압을 전달한다. 프로세스 종료 전 아직 수용하지 못한 입력은 원천 어댑터에서 재공급해야 한다.
