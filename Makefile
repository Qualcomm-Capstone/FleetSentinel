# FleetSentinel — 로컬 인프라 오케스트레이션
# 인프라 검증: make up → make topics → make smoke
#
# 신호·객체 메타데이터 경로는 차량(WAL) → gRPC 게이트웨이 → Kafka 까지 이어져 있다:
#   make up → make topics → make certs → make gateway → make ship
# MQTT 회귀 검증: make mqtt-test → make mqtt-integration-test
# 설계 = docs/sdd.md
COMPOSE := docker compose -f infra/docker-compose.yml
export COMPOSE
STOP_AFTER_MS ?= 30000
MONITOR_STALE_MS ?= 5000
CHECKPOINT_MS ?= 10000
BATCH_SIZE ?= 1000
ALERT_GROUP ?= fleetsentinel-alerts
STORAGE_GROUP ?= fleetsentinel-storage

.DEFAULT_GOAL := help
.PHONY: mqtt-test mqtt-integration-test
mqtt-test: ## MQTT 수집 정책·보존·명령·저장 확인 단위 테스트
	cd exploration && PYTHONPATH=. .venv/bin/python -m pytest -q tests/test_mqtt_collection.py tests/test_mqtt_edge_runtime.py

mqtt-integration-test: ## 격리된 MQTT/Kafka 컨테이너로 mTLS·ACL·재전송·저장 확인 검증
	cd exploration && RUN_MQTT_INTEGRATION=1 PYTHONPATH=. .venv/bin/python -m pytest -q tests/test_mqtt_integration.py

.PHONY: help up down restart ps logs topics smoke clean pull config ha-demo \
        certs gateway gateway-test ship ship-impersonate verify-kafka \
        mcap ship-segments ch-schema flink-build flink-submit flink-test k8s-render \
        api api-build dashboard

help: ## 명령 목록
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

config: ## compose 문법 검증
	@$(COMPOSE) config -q && echo "compose config OK"

pull: ## 이미지 사전 당기기
	$(COMPOSE) pull

up: ## 스택 기동 (헬시까지 대기)
	@$(COMPOSE) up -d --wait; rc=$$?; $(COMPOSE) ps; exit $$rc

down: ## 스택 종료 (볼륨 유지)
	$(COMPOSE) down

clean: ## 스택 종료 + 볼륨/데이터 삭제
	$(COMPOSE) down -v

restart: down up ## 재기동

ps: ## 상태
	$(COMPOSE) ps

logs: ## 로그 팔로우 (make logs S=kafka1)
	$(COMPOSE) logs -f $(S)

topics: ## Kafka 토픽 부트스트랩 (RF=3 / min.insync.replicas=2)
	bash scripts/create-topics.sh

smoke: ## 인프라 수용 스모크 테스트 (전 서비스 healthy + 토픽 + ClickHouse + MinIO)
	bash scripts/smoke.sh

ha-demo: ## Kafka HA broker-kill 데모 (ADR-009, 별도 토픽 사용)
	bash scripts/ha-demo.sh

# ── 수집 게이트웨이 ────────────────────────────────────────────────────────
# 개발용 사설 PKI다. 공개 CA는 vehicle-0042 같은 이름을 발급해주지 않는다(SDD S-9).
# 제조 프로비저닝·로테이션·폐기는 스코프 밖 — SDD L-7.
PKI ?= ./pki
JAVA21 ?= $(shell /usr/libexec/java_home -v 21 2>/dev/null)

certs: ## 개발용 사설 CA + 차량 인증서 발급 (pki/)
	PKI_DIR=$(PKI) bash scripts/gen-certs.sh vehicle-0001 vehicle-0002

gateway-test: ## 게이트웨이 테스트 — 단위 + 통합 (임베디드 Kafka로 mTLS·사칭 차단까지)
	@# verify여야 한다. *IT는 surefire가 아니라 failsafe가 돌린다.
	cd gateway && JAVA_HOME=$(JAVA21) mvn -B verify

gateway: ## 게이트웨이 기동 (gRPC 9090 mTLS · 관리 HTTP 8082)
	cd gateway && JAVA_HOME=$(JAVA21) mvn -B -q package -DskipTests
	FLEETSENTINEL_PKI=$(PKI) \
	KAFKA_BOOTSTRAP=localhost:29092,localhost:29093,localhost:29094 \
	$(JAVA21)/bin/java -jar gateway/target/gateway-0.1.0.jar

ship: ## nuScenes 신호·객체 메타데이터 → WAL → 게이트웨이 → Kafka 종단 재생
	cd exploration && PYTHONPATH=. .venv/bin/python scripts/ship_to_gateway.py \
	  --dataroot ../data/nuscenes --pki ../$(PKI) --vehicle vehicle-0001 --scenes 2

mcap: ## nuScenes 장면 → MCAP 클립 변환 (중량 경로 입력)
	cd exploration && PYTHONPATH=. .venv/bin/python scripts/convert_scenes.py \
	  --dataroot ../data/nuscenes --out ../data/mcap --scenes 3

ship-segments: ## MCAP 클립 → MinIO(presigned) → segment-ref → Kafka
	cd exploration && PYTHONPATH=. .venv/bin/python scripts/ship_segments.py \
	  --pki ../$(PKI) --vehicle vehicle-0001 --mcap-dir ../data/mcap $(ARGS)

# ── Flink 파이프라인 ───────────────────────────────────────────────────────
FLINK_JAR := flink-pipeline/target/flink-pipeline-0.1.0.jar
# nuScenes fixture의 지도 범위를 감싼 개발용 사각 경계다. 실제 ODD 정책이 아니다.
ODD_BOUNDS ?= singapore-onenorth=1.2980,1.2990,103.7882,103.7887;boston-seaport=42.3445,42.3518,-71.0507,-71.0341

ch-schema: ## ClickHouse 스키마 + FINAL 뷰 적용
	$(COMPOSE) exec -T clickhouse clickhouse-client --user fleet --password fleet \
	  --multiquery < infra/clickhouse/001-schema.sql
	@echo "적용 완료 — 애플리케이션은 _raw 가 아니라 뷰를 본다(L-14)"

flink-test: ## Flink 잡 테스트 (Python 이식 대조 포함)
	cd flink-pipeline && JAVA_HOME=$(JAVA21) mvn -B test

flink-build: ## 잡 JAR 빌드
	cd flink-pipeline && JAVA_HOME=$(JAVA21) mvn -B -q package -DskipTests
	@ls -la $(FLINK_JAR)

flink-submit: flink-build ## 알림·ClickHouse 적재를 독립 Flink 잡으로 제출
	docker cp $(FLINK_JAR) fleet-jobmanager:/tmp/job.jar
	$(COMPOSE) exec -T jobmanager flink run -d \
	  -c io.fleetsentinel.pipeline.AlertPipeline /tmp/job.jar \
	  --bootstrap kafka1:9092 --topic telemetry.records \
	  --alert-topic fleet.alerts --odd-bounds "$(ODD_BOUNDS)" \
	  --group $(ALERT_GROUP) \
	  --stop-after-ms $(STOP_AFTER_MS) --monitor-stale-ms $(MONITOR_STALE_MS) \
	  --checkpoint-ms $(CHECKPOINT_MS)
	$(COMPOSE) exec -T jobmanager flink run -d \
	  -c io.fleetsentinel.pipeline.StoragePipeline /tmp/job.jar \
	  --bootstrap kafka1:9092 --topic telemetry.records \
	  --group $(STORAGE_GROUP) \
	  --clickhouse "jdbc:ch://clickhouse:8123/fleet" \
	  --clickhouse-user fleet --clickhouse-password fleet \
	  --checkpoint-ms $(CHECKPOINT_MS) --batch-size $(BATCH_SIZE)

# ── 관제 API ───────────────────────────────────────────────────────────────
api-build: ## API JAR 빌드
	cd api && JAVA_HOME=$(JAVA21) mvn -B -q package -DskipTests
	@ls -la api/target/api-0.1.0.jar

api: api-build ## API 기동 (:8080) — ClickHouse 질의 + telemetry/alert SSE
	CLICKHOUSE_URL="jdbc:ch://localhost:8124/fleet" \
	KAFKA_BOOTSTRAP_SERVERS="localhost:29092,localhost:29093,localhost:29094" \
	$(JAVA21)/bin/java -jar api/target/api-0.1.0.jar

dashboard: ## 대시보드를 **실 API** 로 띄운다 (VITE_API 없으면 목업)
	cd frontend && VITE_API=http://localhost:8080 npm run dev

k8s-render: ## k8s 매니페스트 렌더 (적용은 하지 않는다)
	@kubectl kustomize k8s/overlays/local | grep -c '^kind:' | \
	  xargs -I{} echo "{}개 리소스 렌더 OK — 실제 클러스터 검증은 안 했다(문법까지다)"

verify-kafka: ## Kafka 적재분을 꺼내 Avro 디코딩 + 결번·채널 대조 (BOOT=<boot_id>)
	@# "보냈다"가 아니라 "저장 계층이 실제로 받았다"를 확인한다.
	@test -n "$(BOOT)" || { echo "BOOT=<boot_id> 가 필요하다 — make ship 출력에 찍힌다"; exit 1; }
	cd exploration && PYTHONPATH=. .venv/bin/python scripts/verify_kafka.py \
	  --boot-id $(BOOT) --vehicle vehicle-0001

ship-impersonate: ## 사칭 시도 — PERMISSION_DENIED로 끊겨야 정상 (SDD S-9)
	@# 거절이 곧 성공이다. 재생기는 끊기면 1을 돌려주므로 여기서 뒤집는다 —
	@# 통과해버리면 그때가 실패다.
	@cd exploration && PYTHONPATH=. .venv/bin/python scripts/ship_to_gateway.py \
	  --dataroot ../data/nuscenes --pki ../$(PKI) \
	  --vehicle vehicle-0001 --claim vehicle-0002 --scenes 1 2>&1 \
	  | tee /dev/stderr | grep -q "PERMISSION_DENIED" \
	  && echo "✅ 사칭이 차단됐다" \
	  || { echo "❌ 사칭이 통과했다 — 신원 바인딩이 깨졌다"; exit 1; }
