# MQTT 수집 계약

차량·bridge·관제 명령 처리기가 공유하는 수집 정책, 저장 확인, 재전송 계약을 정의한다.
설계가 달라진 배경과 gRPC/MQTT 선택 근거는 [MQTT 전환 문서](mqtt-migration.md)를 먼저 읽는다.

현재는 로컬에서 선택적으로 실행하는 경로다. 클라우드 HA·대규모 수용량·비용 절감률은 아직 검증하지 않았다.

## 1. 수집 정책 설정

기본값은 정상 상태 보고 최대 10초, 변화량 보고 최소 1초, 집중 관찰 1초다.
기본 선택 채널은 `ego_pose`, `fleet_status`이며, 배터리 등 추가 채널·변화량은 정책으로 지정한다.
센서 자체 수집률을 유지하면서 **클라우드로 전송할 샘플을 선택**한다.
보고는 새로운 센서 관측을 전제로 한다. 센서가 멈췄을 때 과거 값을 새 관측처럼 재발행하지 않는다.

초기 정책 예 (`--policy`로 읽는 JSON):

```json
{
  "version": 0,
  "normal_ms": 10000,
  "min_ms": 1000,
  "focus_ms": 1000,
  "channels": ["ego_pose", "fleet_status", "zoe_veh_info"],
  "focus_channels": ["ego_pose", "fleet_status", "ms_imu"],
  "channel_intervals_ms": {"zoe_veh_info": 60000},
  "thresholds": {"speed_mps": 2.0, "battery_level": 5.0},
  "history_ms": 30000,
  "anomaly_focus_ms": 30000
}
```

이 값은 검증된 안전·배터리 정책이 아니다. 실제 신호 이름·단위·허용 지연에 맞춰 정해야 한다.
`focus_channels`를 지정하면 그 채널만 고빈도로 전환하고 나머지 기본 채널은 정상 주기를 유지한다.
집중 관찰에만 추가한 채널은 만료 후 다시 로컬 기록만 한다. 생략하면 기본 채널 모두에 고빈도 주기를 적용한다.
관제자의 `focus_until_ms`와 이상 상황의 유지 시각은 별도로 영속 저장한다. 둘 중 하나라도 유효하면
집중 관찰 주기를 적용한다. 직접 관찰 중 발생한 첫 이상도 이력을 선택하고, 기존 관찰 시간을 줄이지 않는다.
이상 에피소드는 마지막 이상 보고부터 `anomaly_focus_ms` 동안 유지하며, 같은 에피소드에서는 이력을
반복 선택하지 않는다. 정책 변경으로 활성 이상 에피소드를 취소하지 않으며, 보고 주기·채널은 최신 정책을 따른다.
`fleet_status`는 기존 Flink 계약대로 같은 시각의 `speed_mps`, `mission_active`, `planned_stop`,
`sensor_healthy`, `mission_id`가 필요하다. 서로 다른 시각의 값을 임의로 합치지 않는다.
최초 이상 발견은 선택한 상태 신호의 클라우드 탐지 또는 외부 차량 판단기의 `anomaly=true` 입력에 의존한다.
이 변경에서 새로운 차량 이상 탐지 알고리즘을 구현한 것은 아니다.

## 2. 저장·ACK·중복 제거의 상세 계약

- 기존 gRPC WAL 파일을 변경하거나 자동 변환하지 않는다. MQTT 경로는 SQLite WAL + `synchronous=FULL`로
  시퀀스, outbox, 배치, 정책 명령 처리를 트랜잭션에 넣는다. 한 DB는 한 차량·한 프로세스만 소유한다.
- 일반 신호는 이미 입력 큐에 도착한 최대 32건을 한 트랜잭션으로 커밋한다. 배치를 채우기 위한
  추가 대기는 없으며, 이상 이벤트는 별도 트랜잭션으로 커밋하고 전송을 시도한다. 배치 실패 시
  DB 변경과 메모리의 보고 주기 상태를 함께 되돌린다. 용량 부족 시 한 건씩 수용을 재시도한다.
- SQLite 본체는 메타데이터·인덱스를 포함해 기본 2 GiB로 제한한다(`--max-db-bytes`). 기존 DB가
  지정한 상한보다 크면 시작을 거부한다. WAL 자동 체크포인트는 1,000페이지이며, WAL 재사용 시
  남겨둘 파일 크기는 8 MiB다. 활성 WAL은 이보다 커질 수 있어 별도 디스크 여유 공간이 필요하다.
  `diagnostic.storage`는 DB 할당량·재사용 가능 공간·DB 상한·WAL 크기·파일시스템 여유 공간을 제공한다.
- 로컬 ring은 기본 1시간 또는 payload 256 MiB 중 먼저 도달하는 상한으로 제한한다. 상세 이력은
  이상 발생 시 직전 구간 또는 `history_request`로 선택한 보존 구간만 업로드한다. ring 보존량은
  전체 SQLite 파일 크기와 다르며, SQLite의 파일 재사용·checkpoint 및 디스크 여유 공간도 필요하다.
- 이상 발생 시 event·집중 관찰 정책·이력 선택 목록을 먼저 커밋한다. 선택한 이력은 ring에 고정하고,
  별도 트랜잭션에서 최대 128건씩 history outbox로 옮긴다. outbox 용량이 부족하면 남은 이력을
  유지하고 다음 실행 주기에 재시도한다. 이력 이동과 선택 목록 삭제는 함께 커밋한다.
  고정된 이력은 보관기간이 지나도 유지하며 ring 용량에 포함된다. 용량 압박 시 일반 로컬 샘플을
  먼저 삭제하므로 새로 확보할 수 있는 이력 구간이 짧아질 수 있다. 기존 고정 이력보다 ring 한도를
  낮추지 않는다. `diagnostic.deferred_history_records`로 대기량을 확인한다.
- `history_request`도 선택 목록과 명령 처리 결과를 함께 저장하고 같은 재시도 경로를 사용한다.
  `applied`는 이력 구간 선택의 영속 저장을 뜻한다. 실제 전송 완료는 application ACK로 확인한다.
- outbox는 기본 payload 256 MiB다. 상한이나 디스크 오류가 발생하면 **실패를 호출자에 반환**한다.
  미확인 레코드를 임의 삭제하지 않는다. 센서 어댑터는 이 실패를 운영 경보/입력 역압으로 처리해야 한다.
  디스크 고장·용량 초과 후 수용하지 못한 입력까지 무유실을 보장하지 않는다.
  이력 이동의 용량 부족은 재시도하지만, event 자체를 저장할 공간이 없는 경우에는 수용에 실패한다.
  CLI는 용량 부족 시 미커밋 입력 배치를 대기시키고 추가 입력 처리를 중단한다. 전송·ACK·관제 명령은
  계속 처리하며, ACK로 공간이 확보되면 대기 입력을 재시도한다. 입력 대기 중에는 이력의 outbox 이동을
  잠시 중단한다. 대기·재개는 로그로 남긴다. stdin 입력 큐는 최대 128건, 처리 중인 배치는 최대 32건이며
  생산자에 역압을 전달한다. SQLite 공간 부족 시 해당 트랜잭션을 롤백하고 입력을 유지한다.
  실제 공간이 소진되면 새 전송 배치·명령 결과·ACK 처리에도 실패할 수 있어 저장 공간 확보가 필요하다.
  이 입력 큐와 수용 실패 입력은 메모리에 있으므로, 수용 전 데이터의 프로세스 종료 복구는 원천 어댑터의
  보존·재공급이 필요하다. 배치 크기를 초과하는 입력이나 실제 디스크 장애는 운영자 조치 대상이다.
  배치 한도보다 큰 이력이 대기열 앞에 있으면 이동이 중단되므로 대기량과 입력 크기를 확인한다.
- `event`, `status`, `history`마다 시퀀스와 in-flight 배치를 분리한다. 배치는 최대 128건·64 KiB이고,
  stream별 한 배치씩 공정하게 보낸다. 각 배치의 ID는 내용 해시여서 재시작·재전송에도 바뀌지 않는다.
- MQTT PUBACK은 outbox를 삭제하지 않는다. 차량·boot·stream·batch ID·seq 범위가 모두 일치하는
  application ACK를 받은 뒤 해당 배치만 삭제한다. 일부 Kafka 쓰기만 성공했다면 전체 배치를 재전송한다.
- ACK는 **Kafka 저장 확인**이지 ClickHouse 반영 확인이 아니다. 하류 장애 시 Kafka retention보다
  lag가 오래 지속되면 별도 복구가 필요하다. 잘못된 Avro/업무 데이터는 기존 Flink DLQ 처리 대상이다.
- bridge는 `(vehicle, source_boot, stream)`에서 UUIDv5 boot ID를 만들어 기존 ClickHouse의
  `(vehicle_id, boot_id, seq)` 키를 유지한다. 재전송 키는 동일하고 서로 다른 stream은 충돌하지 않는다.
  Flink의 정지 판정 연속성은 별도 `source_boot_id`로 확인한다. 같은 원천 세션에서 status/event를
  전환해도 정지 판정은 이어지고, 원천 세션이나 임무가 바뀌면 새 관측 구간을 시작한다.
- MQTT의 dedup window 밖 지연 레코드는 유실 방지를 위해 저장 계층으로 통과시킨다. 중복은
  기존 ReplacingMergeTree/FINAL 조회로 처리한다. 모든 구간의 exactly-once를 주장하지 않는다.

집중 관찰 만료/정책 변경 시 다음 관측이 새 주기를 바로 보고한다. Flink 미수신 기준은 기존 설정과
`3 × 보고 간격` 중 큰 값이다. MQTT history와 해당 최신성 범위를 벗어난 상태 재전송은 현재 경보를
갱신하지 않는다. 센서·클라우드 시계 동기화가 필요하며 5초를 넘는 미래 시각도 실시간 판단에서 제외한다.
3배와 5초는 구현 기본값이지 도메인 안전 기준이 아니다. 상태의 저장 시간과 발생 시간은 혼동하지 않는다.

### MQTT 업로드 바이너리 형식과 호환성

새 업로드 배치는 v2 바이너리를 사용한다. `FSMQ` ASCII 4바이트 + 버전 `0x02` 1바이트 +
SHA-256 digest 32바이트 + [`mqtt-batch-v2.avsc`](../schemas/mqtt-batch-v2.avsc)의 schemaless Avro
본문 순서다. Avro Object Container File 헤더나 스키마를 매번 전송하지 않는다. 이 스키마의 필드
순서·타입·enum 순서는 고정하며, 호환되지 않는 변경은 새 프레임 버전을 사용해야 한다.
센서 payload는 base64 없이 `bytes`로 담는다. 배치 ID는 `SHA-256(FSMQ + 0x02 + Avro 본문)`의
소문자 hex 표현이며, 프레임에는 hex 문자열 대신 digest 바이트를 넣는다. 해시는 내용 식별과
오염 검사용이며 송신자 인증을 대체하지 않는다. 인증은 기존 mTLS·ACL로 수행한다.

bridge는 기존 v1 JSON/base64와 v2 바이너리를 모두 받는다. SQLite `batches.wire`에 이미 저장한
v1 배치는 재시작 후에도 v1 형식·기존 배치 ID로 재전송한다. ACK로 해당 배치가 삭제된 뒤 생성하는
배치부터 v2를 사용한다. ACK·관제 명령·reported는 기존 JSON 계약을 유지하며, 차량 입력 JSONL도
변경하지 않는다. Kafka의 Avro 레코드·키·헤더와 Flink 입력 계약은 동일하다.

두 버전 모두 128건·64 KiB 제한, 연속 시퀀스, payload 해시와 토픽 신원 검사를 적용한다.
v2는 잘린 프레임·알 수 없는 버전·후행 바이트·비정규 인코딩을 거절한다. 전체 MQTT 패킷의
토픽·프로퍼티 비용은 64 KiB payload 상한과 별도다. 압축이나 배치를 채우기 위한 대기는 추가하지 않는다.
정규 인코딩은 최소 길이 Avro 정수, records 배열의 단일 양수 count 블록과 종료 `0`, 스키마 필드·enum
순서를 사용한다. 다른 언어의 송신기도 같은 표현을 사용해야 재직렬화한 내용 해시가 일치한다.

직렬화 크기 예시: `vehicle-001`, boot ID 32자, status, seq 100000부터 연속, 보고 간격 10000ms,
signal payload 500B를 사용하면 1건 배치는 v1 932B → v2 594B(36.3% 감소), 10건 배치는
7583B → 5175B(31.8% 감소)다. 업로드 MQTT payload만 비교한 값으로 ACK·TLS·TCP/IP·재전송과
실제 이동통신 요금은 포함하지 않는다. 실제 센서 payload의 평균 크기를 가정한 수치도 아니다.

### DLQ 저장 형식과 재처리

`telemetry.dlq`의 새 메시지는 [`dlq-envelope.avsc`](../schemas/dlq-envelope.avsc)의 Avro Object
Container File이며, 메시지마다 writer schema와 레코드 한 건을 담는다. `format_version=2`다.
원본 value·key·헤더(순서·중복·null 포함), topic·partition·offset과 검증된 차량·boot·seq·stream 정보를
보존한다. `processing_time`은 UTC epoch 마이크로초다. 헤더 검증에 실패한 레코드도 이 경로로 격리한다.

기존 구분자 텍스트 DLQ에는 원본이 없으므로 새 형식으로 복구할 수 없다. 소비자는 이전 텍스트와
새 Avro 형식을 구분해야 한다. 새 포맷은 schema를 포함해 메시지 크기가 늘어난다.
DLQ 전송 실패는 잡 실패·체크포인트 복구 대상으로 남기며, at-least-once 전송에 따른 중복은 가능하다.
대사는 원본 topic·partition·offset으로 수행한다.

재처리 시 오류를 수정하고 별도 replay topic과 dedup 상태를 사용하는 저장 경로로 보낸다.
기존 입력 topic에 같은 ID를 다시 넣으면 앞단 dedup에 걸릴 수 있다. 경보 경로는 재처리 대상에서
분리하고, 저장 식별자는 유지한다. 자동 재처리 CLI와 운영 환경 복구 시험은 후속 작업이다.

## 3. 입력·명령 계약

차량 입력은 `schemas/vehicle-signal.avsc`의 신호 JSON 또는
`{"signal": {신호 필드들}, "anomaly": false}` 형식이다.
timestamp는 실제 관측의 epoch microseconds이며 과거 시각의 데이터는 저장하되 현재 경보에서 제외한다.
입력 어댑터의 구현은 [mqtt_ingest.py](../exploration/scripts/mqtt_ingest.py)에 있다.

관제 명령은 `command_id`, `vehicle_id`, `type`, `expires_at_ms`를 포함한다.
만료는 제출 시점부터 최대 5분이다.

| 명령 | 추가 필드 | 적용 결과 |
|---|---|---|
| `collection_policy` | 증가하는 `version`, `policy`. `focus_until_ms`는 명령 만료 이내 | 유효한 정책과 처리 결과를 함께 저장 |
| `history_request` | 로컬 수신 시각 기준 `start_ms`, `end_ms` | 현재 보존 중인 구간을 선택. 요청 구간 전체의 존재는 보장하지 않음 |
| `clip_request` | 센서 발생 시각 기준 `start_us`, `end_us` | 최대 60초 원본 구간의 요청·파일 pin·처리 결과를 함께 저장 |
| `clip_status` | 원래 `clip_request`의 command ID인 `request_id` | 현재 확보 구간·누락 구간·파일별 완료 상태 조회 |
| `diagnostic` | 없음 | outbox·이력 이동 대기 건수, 정책 버전, 관제 관찰·이상 유지 시각 |

dispatcher와 차량이 같은 정책 검증을 수행한다. 잘못된 정책은 발행 전에 거절하며,
차량에 도착한 경우에도 `rejected` 결과를 저장·회신하고 다음 명령을 처리한다.
명령과 결과는 DB에 보존하고, 재시작 후 미완료·미만료 명령만 재전송한다.
실행 명령에는 retained 메시지를 사용하지 않는다.

### 원본 클립 명령과 완료 확인

`clip_request`의 시각은 epoch microseconds, 구간은 `[start_us, end_us)`다.
`history_request`는 로컬 수신 시각의 milliseconds를 사용하므로 단위를 섞지 않는다.

```json
{
  "command_id": "clip-incident-42",
  "vehicle_id": "vehicle-001",
  "type": "clip_request",
  "expires_at_ms": 1793000100000,
  "start_us": 1793000000000000,
  "end_us": 1793000030000000
}
```

위 시각은 형식 예시다. 실제 제출 시 만료 시각을 현재 시각부터 최대 5분 이내로 정한다.
같은 명령의 재전송은 저장된 결과를 반환한다. `applied`는 요청과 파일 pin이 영속 저장됐다는
뜻이며 전송 완료를 뜻하지 않는다. 이미 사라진 과거나 아직 기록 중인 사건 후 구간은 부분 결과로 남는다.
차량 이상 입력은 기본적으로 사건 10초 전부터 20초 후까지 같은 원본 선택 경로를 사용한다.
사건 시각은 신호의 `sensor_time`이며 원본용 에피소드 상태를 별도로 영속 기록한다.
기존 에피소드의 사건 시각 이상이고 20초 후 경계보다 앞선 입력만 같은 선택을 재사용한다.
더 이른 사건이나 20초 후 경계부터의 사건은 새 구간을 선택하며, 같은 사건 시각의 재전송은 중복 선택하지 않는다.
JSONL 이상 입력과 원본 어댑터는 같은 시각 축을 사용해야 한다. 원본 재생은 원천 시각을 현재 차량
시각으로 옮기므로 함께 주입하는 이상 신호도 그 시각에 맞춘다.

진행 상태를 새로 조회할 때는 새로운 `command_id`로 `clip_status`를 보내고,
`request_id`에 원래 `clip_request.command_id`를 넣는다. 조회 명령도 중복 처리 결과를 보존하므로
같은 조회 command ID를 반복하면 그 명령의 기존 결과를 받는다.

```json
{
  "command_id": "clip-status-42-1",
  "vehicle_id": "vehicle-001",
  "type": "clip_status",
  "expires_at_ms": 1793000100000,
  "request_id": "clip-incident-42"
}
```

차량의 요청 상태에는 `requested`, `coverage`, `missing`, `partial`, `file_ids`,
`uploaded_file_ids`가 포함된다. 파일 전송 완료와 요청 범위 전체 확보를 각각 확인한다.
`uploads`에서 파일별 상태·재시도·오류·작업 ID를, `clip_upload_states`에서 상태별 건수를 확인한다.
`diagnostic`에는 원본 용량·pin 상태인 `raw`와 원천 재생 상태인 `raw_replay_state`도 포함된다.
원본 바이트는 별도 HTTPS worker가 전송하며, 서버의 객체 검증과 참조 Kafka ACK가 DB에 기록된
`COMPLETED` receipt를 차량이 저장해야 pin이 해제된다. MQTT 명령의 `applied`, MQTT PUBACK,
신호 배치의 application ACK는 이 원본 receipt를 대체하지 않는다.

원본 파일의 MQTT `create.body.request_id`는 차량 파일 ID다. MQTT의 구간 요청 ID와 다른 단위이며,
요청 여러 건이 한 파일을 공유해도 같은 파일을 다시 업로드하지 않는다.
원본 spool은 기본 1시간·128 GiB, 회전은 10초·256 MiB, 전송은 16 MiB part·4 MiB/s·worker 1개다.
선택된 미확인 파일은 자동 만료하지 않으며 용량이 부족하면 새 원본 수용을 중단한다.
삭제는 관리 spool에 등록한 파일로 제한한다. 구체적인 수명주기는 [SDD S-1·S-2](sdd.md#3-해결-방법)를 따른다.

인증서 CN을 MQTT username/clientId로 고정하고 차량별 토픽 ACL을 적용한다.
기존 gRPC의 SAN 기반 인증서와 자동 호환되지 않는다. 운영 환경에는 별도 PKI와 인증서 교체·폐기 정책이 필요하다.

### 원본 업로드 제어와 참조 메타데이터

원본 파일은 HTTPS multipart로 MinIO/S3에 직접 보낸다. 차량과 서버 사이의 작업 생성·상태 조회·
주소 요청·완료 통지는 MQTT로 통일한다. 기존 `/v1/clip-uploads` REST 핸들러와 차량의
`--clip-url`, `--clip-ca`, `--clip-cert`, `--clip-key` 옵션은 제거했다.

| 토픽 | 발행 → 구독 | 내용 |
|---|---|---|
| `fleet/v1/{vehicle_id}/clip/request` | 차량 → 클립 제어 서버 | 작업 명령과 참조 메타데이터 |
| `fleet/v1/{vehicle_id}/clip/response` | 클립 제어 서버 → 해당 차량 | 상관 ID·처리 결과·상태 또는 part URL |

두 토픽은 QoS 1, retain=false다. 차량의 인증서 CN을 broker username/clientId에 고정하고
자기 차량의 request 발행과 response 구독만 허용한다. 응답 발행 권한은 클립 서비스만 갖는다.
서버는 `$share/clip-control/fleet/v1/+/clip/request`를 구독하고 ACL로 보호된 토픽에서 차량 ID를
얻는다. 본문에 주장된 차량 ID나 응답 토픽으로 신원·응답 경로를 바꾸지 않는다.

요청 예시(아래는 파일 전송 이후의 완료 통지):

```json
{
  "version": 1,
  "correlation_id": "attempt-unique-id",
  "operation": "complete",
  "job_id": "01SERVERUPLOADJOBID",
  "body": {
    "upload_id": "multipart-generation-id",
    "parts": [{"part_number": 1, "etag": "part-etag"}]
  }
}
```

응답은 `version`, `correlation_id`, `operation`, `status`, `data`를 담는다. `status`는 HTTP
응답이 아니라 MQTT 본문의 정수 결과 코드다. 200은 연산 성공, 400은 잘못된 입력, 404는 없는 작업
또는 타 차량 작업, 409는 상태·메타데이터·multipart 세대 충돌, 413은 크기 초과, 503은 재시도 가능한
서버 오류다. `status=200`이어도 `data.state=VERIFYING`이면 업로드의 최종 완료가 아니다.

| operation | body / 결과 |
|---|---|
| `create` | 파일 ID `request_id`, 시간 범위·크기·SHA-256·채널·캘리브레이션 → 작업 상태 |
| `status` | 빈 본문 + `job_id` → 현재 작업 상태 |
| `part_urls` | `part_numbers` 최대 32개 → 현재 `upload_id`, part URL, 만료 시각 |
| `complete` | 현재 `upload_id`, 전체 part 번호·ETag → 검증 접수 또는 기존 상태 |
| `cancel` | 빈 본문 + `job_id` → 검증 시작 전 취소 접수 또는 기존 취소 상태 |

`create` 이외에는 `job_id`가 필수다. 작업 상태는 `id`, `state`, `upload_id`, `blob_uri`,
`size_bytes`, `sha256`, `part_size_bytes`, `error`다. 필드 대응은 [데이터 설계 §5.3](data-design.md)를 따른다.
서버가 메타데이터와 저장된 객체를 검증한 뒤 `segment-ref` Avro와 Kafka outbox를 만든다.
참조의 Kafka ACK를 DB에 기록해야 `COMPLETED`가 된다. 이 상태는 ClickHouse 반영 완료를 뜻하지 않는다.

**유실·중복·재시작 계약:**

- 차량은 최초 요청 전에 불변 manifest를 SQLite에 저장한다. 같은 차량·파일 `request_id`는 같은
  서버 작업으로 수렴한다. 같은 ID에 다른 메타데이터를 보내면 409다.
- 각 호출은 새 `correlation_id`를 사용한다. 응답 토픽·상관 ID·operation이 모두 일치해야 수용한다.
  이전 시도의 늦은 응답과 중복 응답은 버린다. correlation ID는 영속 멱등 키가 아니다.
- 한 업로드 worker가 한 번에 요청 하나를 대기한다. 응답 제한은 15초이며 MQTT owner thread는
  계속 신호·명령·ACK를 처리한다. 시간 초과·단절은 영속 backoff로 재시도한다.
- 서버가 DB 반영 후 응답을 잃어도 같은 `create`/`complete`를 재처리할 수 있다. 서버 응답을
  broker가 ACK했다는 이유로 DB 작업이나 차량 pin을 지우지 않는다. 상태 조회로 최종 결과를 복구한다.
- 같은 완료 manifest는 멱등이며 변경된 manifest나 이전 `upload_id`는 409다. 차량은 상태를
  재조회하고 세대 변경 시 기존 ETag를 비운 뒤 part를 재전송한다. 서명 URL은 시도마다 새로 받고 저장하지 않는다.
- 파일 크기·체크섬·작업 ID가 일치하는 `COMPLETED` 결과를 로컬 DB에 저장한 뒤에만 pin을 해제한다.
- 서버의 DB 트랜잭션·lease·Kafka outbox는 기존 계약을 유지한다. Kafka ACK 직후 서버가 죽으면
  같은 참조가 재발행될 수 있으며 하류는 기존 `(vehicle_id, boot_id, seq)`로 중복을 처리한다.

복구 전제는 차량 spool·SQLite, 서버 PostgreSQL과 객체 저장소가 보존되고 브로커 접속 주소·인증서가
유효한 상태로 서비스가 재개되는 것이다. `COMPLETED`는 카탈로그 저장 확인이 아니므로 Flink 장애가
Kafka 보존 기간을 넘으면 참조가 조회되지 않을 수 있다. 완료된 outbox를 카탈로그와 자동 대사해
재발행하는 기능은 없다. 관련 장애 주입 결과와 미검증 범위는 [SDD §6.3](sdd.md)에 기록한다.

클립 제어 payload 상한은 1 MiB다. 기존 신호 배치 상한 64 KiB는 유지한다. 브로커의
`message_size_limit=1048576`, `max_packet_size=1049600`과 차량 CONNECT 수신 상한을 맞춘다.
서버는 2개 요청 worker·16개 대기 요청으로 제한하고 초과 요청은 차량의 시간 초과 재시도로 복구한다.
새 Java adapter는 MQTT 3.1.1, 차량은 MQTT 5를 사용하며 상관 정보는 공통 JSON 본문에 있다.

**서버 설정:** `CLIP_MQTT_URI=ssl://localhost:8883`, `CLIP_MQTT_CLIENT_ID=clip-service`,
`MQTT_PKI=./mqtt-pki`를 사용한다. PEM 디렉터리에는 CA와 서비스 CN 이름의 `.crt`·`.key`가 필요하다.
HTTPS 8443 TLS 설정은 유지하지만 클립 REST 핸들러는 제공하지 않는다. 관리 포트는 8082다.
로컬 PKI 생성 스크립트는 `clip-service`, `clip-service-2` 인증서를 생성한다. 운영 PKI를 자동 교체하지 않는다.

복수 서버는 같은 PostgreSQL과 서로 다른 서비스 인증서 CN을 사용한다. 브로커가 CN을 clientId로
고정하므로 같은 인증서를 두 인스턴스에서 사용하면 연결을 빼앗는다. 기존 Kubernetes Deployment를
그대로 적용하는 것만으로 MQTT 제어 서버가 준비되지는 않는다. 브로커·ACL과 인스턴스별 인증서
주입이 별도로 필요하며, 이 변경에서는 Kubernetes 배포 형태와 기존 TLS 경계를 변경하지 않는다.

## 4. 전환 시 지켜야 할 계약

- v2 업로드 전환은 **bridge 먼저, 차량 나중** 순서다. 새 bridge와 차량 배포에는
  `schemas/mqtt-batch-v2.avsc`를 포함한다. 구 bridge는 v2를 읽지 못한다. v2 배치가 남은 차량을
  구버전으로 되돌리면 SQLite의 배치를 읽을 수 없으므로, v2를 이해하는 버전을 유지하며 pending
  outbox를 drain하고 생산을 중단한 뒤 롤백한다. 저장된 배치를 임의 변환하거나 삭제하지 않는다.
- MQTT 입력 전에 새 Flink를 적용한다. 구버전은 stream 구분과 history의 경보 제외를 처리하지 못한다.
- `Envelope`/`Decoded` 직렬화와 keyed state 범위가 바뀌었다. 기존 savepoint의 호환성 또는 offset handover를 검증해야 한다.
- 차량 상태에 추가한 원천 boot가 복원 상태에서 비어 있으면 첫 관측에서 정지 판정을 새로 시작한다.
- 롤백 중에도 미확인 SQLite outbox를 보존한다. 기존 gRPC WAL로 교차 재생하지 않는다.
- 과거 데이터를 다시 처리할 때는 경보 전달을 격리한다.
- 원본 MQTT 제어 전환 전에는 서버 클립 참조를 센서 SeqWindow 접근 전에 처리하는 Flink 버전을 적용한다.
- 기존 SegmentUpload의 multipart와 차량 WAL 참조를 drain한 뒤 비활성화한다. gRPC Ingest는 유지한다.
- 원본 업로드 롤백에서도 차량 manifest·pin과 PostgreSQL 작업·참조 outbox를 보존한다.
- REST 제어 버전에서 MQTT 제어 버전으로 전환할 때는 대상 차량의 업로드 worker를 정지하고
  진행 상태를 보존한다. 브로커의 클립 토픽 ACL·메시지 상한과 서버 MQTT 인증서를 먼저 준비한 뒤
  새 서버·차량을 함께 적용한다. 새 서버는 이전 REST 클립 API를 제공하지 않으므로 혼합 운영을
  자동 지원한다고 가정하지 않는다.
- 전환 전 SAN 기반 차량 ID와 새 MQTT 인증서 CN의 차량 ID가 동일해야 한다. SQLite의 `clip_uploads`
  manifest·job·parts와 서버 DB를 재사용해 같은 작업을 재개한다. 전송 시도의 상관 ID만 새로 생성한다.
- 서버 서비스를 복제할 때 각 인스턴스의 인증서 CN과 ACL을 따로 준비한다. 인증서 하나를 공유한
  Kubernetes 복제 배포는 지원하지 않는다. 운영 브로커·인증서 배포는 별도 검증 대상이다.

단계별 적용·롤백은 [SDD §5](sdd.md), 검증 범위와 남은 운영 과제는 [SDD §6](sdd.md)를 따른다.
`make mqtt-test`는 단위 테스트, `make mqtt-integration-test`는 격리된 MQTT·Kafka·ClickHouse 통합 테스트다.
