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
  CLI는 용량 부족 시 해당 입력 한 건을 대기시키고 추가 입력 처리를 중단한다. 전송·ACK·관제 명령은
  계속 처리하며, ACK로 공간이 확보되면 대기 입력을 재시도한다. 입력 대기 중에는 이력의 outbox 이동을
  잠시 중단한다. 대기·재개는 로그로 남긴다. stdin 입력 큐는 최대 128건이며 생산자에 역압을 전달한다.
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
| `diagnostic` | 없음 | outbox·이력 이동 대기 건수, 정책 버전, 관제 관찰·이상 유지 시각 |

dispatcher와 차량이 같은 정책 검증을 수행한다. 잘못된 정책은 발행 전에 거절하며,
차량에 도착한 경우에도 `rejected` 결과를 저장·회신하고 다음 명령을 처리한다.
명령과 결과는 DB에 보존하고, 재시작 후 미완료·미만료 명령만 재전송한다.
실행 명령에는 retained 메시지를 사용하지 않는다.

인증서 CN을 MQTT username/clientId로 고정하고 차량별 토픽 ACL을 적용한다.
기존 gRPC의 SAN 기반 인증서와 자동 호환되지 않는다. 운영 환경에는 별도 PKI와 인증서 교체·폐기 정책이 필요하다.

## 4. 전환 시 지켜야 할 계약

- MQTT 입력 전에 새 Flink를 적용한다. 구버전은 stream 구분과 history의 경보 제외를 처리하지 못한다.
- `Envelope`/`Decoded` 직렬화와 keyed state 범위가 바뀌었다. 기존 savepoint의 호환성 또는 offset handover를 검증해야 한다.
- 차량 상태에 추가한 원천 boot가 복원 상태에서 비어 있으면 첫 관측에서 정지 판정을 새로 시작한다.
- 롤백 중에도 미확인 SQLite outbox를 보존한다. 기존 gRPC WAL로 교차 재생하지 않는다.
- 과거 데이터를 다시 처리할 때는 경보 전달을 격리한다.

단계별 적용·롤백은 [SDD §5](sdd.md), 검증 범위와 남은 운영 과제는 [SDD §6](sdd.md)를 따른다.
`make mqtt-test`는 단위 테스트, `make mqtt-integration-test`는 격리된 MQTT·Kafka·ClickHouse 통합 테스트다.
