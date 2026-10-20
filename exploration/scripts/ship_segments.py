#!/usr/bin/env python3
"""중량 경로 종단 재생기 — MCAP 클립 → 오브젝트 스토리지 → `segment-ref` → Kafka.

    ┌──────────┐  ① Begin(mTLS)   ┌──────────┐
    │  차량     │─────────────────▶│ 게이트웨이 │  presigned URL만 발급
    │          │◀── part_urls ────│          │  (데이터는 안 지나간다)
    │  MCAP    │                  └────┬─────┘
    │  341 MiB │  ② PUT parts          │ ④ Complete → sha256 대조
    │          │──────────────────────┐│
    └────┬─────┘                      ▼▼
         │                     ┌──────────────┐
         │ ③ 의도 파일 fsync    │ 오브젝트      │
         │ ⑤ WAL: SEGMENT_REF  │ 스토리지      │
         ▼                     └──────────────┘
    ⑥ 경량 경로로 segment-ref 발행 → Kafka telemetry.segments

**③이 ②보다 먼저다.** 의도를 durable하게 남겨야 어디서 죽든 고아를 발견할 수 있다.
의도는 **WAL이 아니라 별도 파일**이다 — WAL에 넣으면 게이트웨이로 안 가는 `seq`가 생기고
저장 계층이 그 자리를 결번(=유실)으로 읽는다.

사용:
    PYTHONPATH=. .venv/bin/python scripts/ship_segments.py \\
        --pki ../pki --vehicle vehicle-0001 --mcap-dir ../data/mcap

    # 재개 확인 — 중간에 죽인 뒤 다시 돌리면 남은 파트만 올린다
    ... --resume-only
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from pathlib import Path
from typing import Any, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import grpc  # noqa: E402

from fleetsentinel_ingest.grpc_transport import (  # noqa: E402
    GrpcTransport,
    load_credentials,
)
from fleetsentinel_ingest.proto import ingest_pb2_grpc  # noqa: E402
from fleetsentinel_ingest.segment_upload import (  # noqa: E402
    SegmentUploader,
    UploadState,
)
from fleetsentinel_ingest.shipping import WalShipper  # noqa: E402
from fleetsentinel_ingest.wal import Wal  # noqa: E402

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"


def _require_fastavro():
    try:
        import fastavro
    except ImportError:
        raise SystemExit("fastavro가 필요하다:  .venv/bin/pip install fastavro")
    return fastavro


def _mcap_meta(path: Path) -> dict:
    """MCAP에서 `segment-ref`에 필요한 메타를 뽑는다.

    파일을 직접 읽는다 — 재생기가 원본 데이터셋을 참조하면 "MCAP이 자체충족적이다"라는
    주장([SDD S-6](../../docs/sdd.md))을 스스로 어긴다.
    """
    from mcap.reader import make_reader

    with open(path, "rb") as f:
        summary = make_reader(f).get_summary()
        stats = summary.statistics
        channels = {c.topic for c in summary.channels.values()}
        # 원시 센서 채널만 센다. /vehicle/signal · /perception/objects · /tf/calibration 제외
        sensors = sorted(
            t.rsplit("/", 1)[-1] for t in channels
            if t.startswith(("/camera/", "/lidar/", "/radar/")))
        return {
            "t_start_us": stats.message_start_time // 1000,
            "t_end_us": stats.message_end_time // 1000,
            "sample_count": sum(stats.channel_message_counts.values()),
            "sensor_channels": sensors,
            "calibration": _calibration(path),
        }


def _calibration(path: Path) -> dict:
    """`/tf/calibration` 메시지를 채널별 JSON 문자열 맵으로."""
    from mcap.reader import make_reader

    out: dict = {}
    with open(path, "rb") as f:
        reader = make_reader(f)
        for schema, channel, message in reader.iter_messages(topics=["/tf/calibration"]):
            try:
                d = json.loads(message.data)
            except json.JSONDecodeError:
                continue
            key = d.get("channel") or f"unknown-{len(out)}"
            out[key] = json.dumps(d, ensure_ascii=False)
    return out


def _segment_ref_row(st: UploadState, meta: dict, scene_id: str) -> dict:
    return {
        "segment_id": st.segment_id,
        "scene_id": scene_id or None,
        "blob_uri": st.blob_uri,
        "t_start": meta["t_start_us"],
        "t_end": meta["t_end_us"],
        "sensor_channels": meta["sensor_channels"],
        "size_bytes": st.size_bytes,
        "checksum": st.sha256,
        "sample_count": meta["sample_count"],
        "state": "UPLOADED",
        "drop_reason": None,
        "calibration": meta["calibration"],
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pki", required=True)
    ap.add_argument("--vehicle", default="vehicle-0001")
    ap.add_argument("--claim", default=None)
    ap.add_argument("--target", default="localhost:9090")
    ap.add_argument("--server-name", default="localhost")
    ap.add_argument("--mcap-dir", type=Path, default=Path("../data/mcap"))
    ap.add_argument("--wal", type=Path, default=Path("./wal-segments"))
    ap.add_argument("--limit", type=int, default=None, help="올릴 클립 수")
    ap.add_argument("--part-size", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--resume-only", action="store_true",
                    help="새로 올리지 않고 미완결분만 이어간다")
    ap.add_argument("--fail-after-parts", type=int, default=None,
                    help="이 파트 수만큼 올린 뒤 죽는다 — 재개 검증용")
    args = ap.parse_args()

    fastavro = _require_fastavro()
    ref_schema = fastavro.parse_schema(
        json.loads((SCHEMA_DIR / "segment-ref.avsc").read_text()))

    creds = load_credentials(args.pki, args.vehicle)
    claim = args.claim or args.vehicle
    channel = grpc.secure_channel(
        args.target, creds,
        options=[("grpc.ssl_target_name_override", args.server_name)])
    metadata = (("x-vehicle-id", claim), ("x-boot-id", "segment-control"))

    # 제어 평면 스텁. 메타데이터는 인터셉터가 인증서와 대조한다(SDD S-9).
    class _Stub:
        def __init__(self, inner):
            self._i = inner

        def __getattr__(self, name):
            fn = getattr(self._i, name)
            return lambda req: fn(req, metadata=metadata)

    stub = _Stub(ingest_pb2_grpc.SegmentUploadStub(channel))

    args.wal.mkdir(parents=True, exist_ok=True)
    wal = Wal(args.wal)
    uploader = SegmentUploader(stub, wal, args.wal, part_size=args.part_size)

    print(f"WAL: {args.wal}  boot_id={wal.boot_id}  committed={wal.committed_seq}")

    # ── 미완결 업로드 먼저 ───────────────────────────────────────────────
    pending = uploader.pending()
    if pending:
        print(f"\n미완결 업로드 {len(pending)}건 — 이어서 올린다")
        for st in pending:
            done = len(st.parts)
            print(f"  {st.segment_id}: {done}/{st.part_count} 파트 완료 → 재개")
            st = uploader.resume(st, on_progress=lambda n, t: None)
            print(f"    ✅ 완료 {st.blob_uri}")
            _append_ref(uploader, fastavro, ref_schema, st, args,
                        scene_id=Path(st.mcap_path).stem)
    elif args.resume_only:
        print("미완결 업로드가 없다")

    if args.resume_only:
        return _publish(wal, args, claim, creds)

    # ── 새 클립 업로드 ───────────────────────────────────────────────────
    clips = sorted(args.mcap_dir.glob("*.mcap"))
    if args.limit:
        clips = clips[: args.limit]
    if not clips:
        print(f"MCAP이 없다: {args.mcap_dir}")
        return 1

    print(f"\n클립 {len(clips)}개 · 파트 크기 {args.part_size // 1024 // 1024} MiB")
    uploaded = 0
    for path in clips:
        meta = _mcap_meta(path)
        size_mib = path.stat().st_size / 1024 / 1024
        print(f"\n  {path.name}  {size_mib:.1f} MiB · "
              f"{meta['sample_count']:,}건 · 채널 {len(meta['sensor_channels'])}개")

        t0 = time.monotonic()

        def on_intent(st: UploadState) -> None:
            # 의도는 이미 uploads/<segment_id>.json 에 fsync 됐다. WAL에 넣지 않는다 —
            # 게이트웨이로 안 가는 seq를 WAL에 넣으면 저장 계층이 그 자리를 결번(=유실)
            # 으로 읽는다(segment_upload.py 모듈 독스트링).
            print(f"    intent durable  segment={st.segment_id}")

        def on_progress(n: int, total: int) -> None:
            if n == total or n % 5 == 0:
                print(f"    파트 {n}/{total}")
            if args.fail_after_parts and n >= args.fail_after_parts:
                print(f"    ⚠️ --fail-after-parts={args.fail_after_parts} — 여기서 죽는다")
                os_exit()

        st = uploader.upload(
            path, meta["t_start_us"], meta["t_end_us"],
            meta["sensor_channels"], meta["sample_count"],
            scene_id=path.stem, on_intent=on_intent, on_progress=on_progress)

        dt = time.monotonic() - t0
        print(f"    ✅ {st.blob_uri}  ({size_mib / max(dt, 1e-9):.1f} MiB/s)")
        _append_ref(uploader, fastavro, ref_schema, st, args,
                    scene_id=path.stem, meta=meta)
        uploaded += 1

    print(f"\n업로드 {uploaded}건 완료")
    return _publish(wal, args, claim, creds)


def os_exit():
    """버퍼를 비우고 즉시 죽는다 — SIGKILL과 같은 효과."""
    sys.stdout.flush()
    import os
    os._exit(137)


def _append_ref(uploader: SegmentUploader, fastavro, schema, st: UploadState, args,
                scene_id: str = "", meta: dict | None = None) -> None:
    """업로드가 확정된 뒤 ref를 fsync하고, 그 다음에만 업로드 상태를 지운다."""
    if meta is None:
        meta = _mcap_meta(Path(st.mcap_path))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, _segment_ref_row(st, meta, scene_id))
    seq = uploader.persist_reference(st, buf.getvalue())
    print(f"    ref WAL seq={seq}")


def _publish(wal, args, claim, creds) -> int:
    """④ 경량 경로로 `segment-ref`를 발행한다. `KIND_SEGMENT_INTENT`는 보내지 않는다."""
    transport = GrpcTransport(
        target=args.target, credentials=creds, vehicle_id=claim,
        server_name=args.server_name)
    shipper = WalShipper(wal, transport)
    transport.set_ack_handler(shipper.on_ack)

    to_send = wal.next_seq - 1 - wal.committed_seq
    if to_send <= 0:
        print("발행할 레코드가 없다")
        transport.close()
        return 0

    print(f"\n발행: resume_from={shipper.resume_from} · {to_send}건")
    transport.start(wal.boot_id)
    deadline = time.monotonic() + 60
    while shipper.stats.sent < to_send and time.monotonic() < deadline:
        if transport.stats.terminal_error:
            break
        if shipper.pump(budget=64) == 0:
            time.sleep(0.005)
    transport.close()

    print(f"  전송 {shipper.stats.sent}건 · CACK {transport.stats.acks_received}회 "
          f"· 커밋 {wal.committed_seq}")
    if transport.stats.terminal_error:
        print(f"  스트림 종료: {transport.stats.terminal_error}")
        return 1
    print("  ✅ segment-ref 발행 완료")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
