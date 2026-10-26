/**
 * 개발용 목 스트림 — Vite 미들웨어로 SSE와 REST를 제공한다.
 *
 * 백엔드 없이 화면을 개발할 때 실제 nuScenes 픽스처(`public/fixture/`)를
 * 100ms 프레임으로 배칭해 재생한다. 기본 입력은 초당 약 5,100 레코드다.
 *
 * 브라우저 배칭은 SSE 이벤트 처리 횟수를 줄인다. 목업의 재개는 픽스처 커서로 처리하고,
 * 실제 API 경로는 저장된 데이터를 Last-Event-ID 기준으로 재조회한다.
 *
 * 재현하는 백엔드 계약:
 *   GET /api/stream        SSE. `id:`에 재생 커서(ms)를 실어 Last-Event-ID 재개를 지원한다.
 *   GET /api/alerts        Flink 전이 이벤트와 같은 계약의 개발용 SSE
 *   GET /api/vehicles      차량 로스터
 *   GET /api/clips         클립 카탈로그
 *   GET /api/health        파이프라인 상태 (Kafka lag, DLQ, 체크포인트)
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Connect, ViteDevServer } from "vite";

type Batch = { t: number; n: number; records: Record<string, unknown>[] };
type Fixture = {
  meta: { vehicles: { vehicle_id: string; duration_ms: number }[]; duration_ms: number };
  batches: Record<string, Batch[]>;
  perception: { vehicle_id: string; t: number; [k: string]: unknown }[];
  clips: unknown[];
};

let cache: Fixture | null = null;

function load(root: string): Fixture {
  if (cache) return cache;
  const p = (name: string) => join(root, "public", "fixture", name);
  const read = (name: string) => JSON.parse(readFileSync(p(name), "utf8"));
  cache = {
    meta: read("meta.json"),
    batches: read("batches.json"),
    perception: read("perception.json"),
    clips: read("clips.json"),
  };
  return cache;
}

/** 모든 차량의 배치를 하나의 시간순 타임라인으로 합친다. */
function timeline(fx: Fixture) {
  const events: { t: number; kind: "signal" | "perception"; vehicle_id: string; payload: unknown }[] = [];
  for (const [vehicleId, batches] of Object.entries(fx.batches)) {
    for (const b of batches) {
      events.push({ t: b.t, kind: "signal", vehicle_id: vehicleId, payload: b });
    }
  }
  for (const p of fx.perception) {
    events.push({ t: p.t, kind: "perception", vehicle_id: p.vehicle_id, payload: p });
  }
  events.sort((a, b) => a.t - b.t);
  return events;
}

export function mockStreamPlugin() {
  return {
    name: "fleetsentinel-mock-stream",
    configureServer(server: ViteDevServer) {
      const root = server.config.root;

      const json = (res: Parameters<Connect.NextHandleFunction>[1], body: unknown) => {
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.end(JSON.stringify(body));
      };

      server.middlewares.use("/api/vehicles", (_req, res) => json(res, load(root).meta));
      server.middlewares.use("/api/clips", (_req, res) => json(res, load(root).clips));

      // 파이프라인 상태 — 실제로는 Prometheus/Actuator에서 온다. 여기서는 그럴듯한 변동만.
      let tick = 0;
      server.middlewares.use("/api/health", (_req, res) => {
        tick += 1;
        json(res, {
          kafka_consumer_lag: Math.max(0, Math.round(120 + 90 * Math.sin(tick / 7))),
          dlq_count: 0,
          checkpoint_ms: 900 + Math.round(300 * Math.sin(tick / 5)),
          checkpoint_failed: 0,
          brokers_in_sync: 3,
          updated_at: Date.now(),
        });
      });

      server.middlewares.use("/api/stream", (req, res) => {
        const fx = load(root);
        const events = timeline(fx);

        // 재개 지원: Last-Event-ID(또는 ?from=)가 있으면 그 시점부터 다시 흘린다.
        const url = new URL(req.url ?? "/", "http://localhost");
        const lastId = (req.headers["last-event-id"] as string | undefined) ?? url.searchParams.get("from");
        const speed = Number(url.searchParams.get("speed") ?? "1") || 1;
        let cursor = lastId ? Number(lastId) : 0;
        if (!Number.isFinite(cursor) || cursor < 0) cursor = 0;

        res.writeHead(200, {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-cache, no-transform",
          Connection: "keep-alive",
          "X-Accel-Buffering": "no",
        });

        let idx = events.findIndex((e) => e.t >= cursor);
        if (idx < 0) idx = 0;
        let closed = false;
        req.on("close", () => {
          closed = true;
          clearTimeout(timer);
          clearInterval(heartbeat);
        });

        // 하트비트 — 프록시가 유휴 연결을 끊는 것을 막고, 클라이언트가 생존을 판단할 근거가 된다.
        const heartbeat = setInterval(() => {
          if (!closed) res.write(`: keepalive ${Date.now()}\n\n`);
        }, 15000);

        const startWall = Date.now();
        const startSim = events[idx]?.t ?? 0;
        let timer: NodeJS.Timeout;

        const pump = () => {
          if (closed) return;
          const elapsed = (Date.now() - startWall) * speed;
          const until = startSim + elapsed;

          while (idx < events.length && events[idx]!.t <= until) {
            const e = events[idx]!;
            res.write(`id: ${e.t}\n`);
            res.write(`event: ${e.kind}\n`);
            res.write(`data: ${JSON.stringify({ vehicle_id: e.vehicle_id, ...(e.payload as object) })}\n\n`);
            idx += 1;
          }

          if (idx >= events.length) {
            // 루프 재생 — 실제 재생기의 load 모드와 같은 동작(replay_epoch 상당).
            idx = 0;
            res.write(`event: epoch\ndata: ${JSON.stringify({ at: Date.now() })}\n\n`);
            timer = setTimeout(pump, 500);
            return;
          }
          timer = setTimeout(pump, 40);
        };
        pump();
      });

      server.middlewares.use("/api/alerts", (req, res) => {
        const fx = load(root);
        const vehicleId = fx.meta.vehicles[0]?.vehicle_id ?? "vehicle-0001";
        const phases = [
          ["ODD_EXITED", "WARNING", "차량이 ODD 경계를 벗어났다"],
          ["ODD_RETURNED", "INFO", "차량이 ODD 경계 안으로 복귀했다"],
        ] as const;
        let sequence = 0;
        let closed = false;

        res.writeHead(200, {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-cache, no-transform",
          Connection: "keep-alive",
          "X-Accel-Buffering": "no",
        });
        req.on("close", () => {
          closed = true;
          clearInterval(timer);
          clearTimeout(initialTimer);
        });

        const send = () => {
          if (closed) return;
          const [type, severity, message] = phases[sequence % phases.length]!;
          const now = Date.now();
          const eventId = `mock:${vehicleId}:${type}:${sequence}`;
          res.write(`id: ${eventId}\n`);
          res.write("event: alert\n");
          res.write(`data: ${JSON.stringify({
            eventId,
            type,
            severity,
            vehicleId,
            bootId: "mock-boot",
            sourceSeq: sequence,
            eventTimeMs: now,
            detectedAtMs: now,
            location: "singapore-onenorth",
            latitude: 1.2986,
            longitude: 103.7885,
            message,
          })}\n\n`);
          sequence += 1;
        };

        const timer = setInterval(send, 8_000);
        const initialTimer = setTimeout(send, 1_500);
      });
    },
  };
}
