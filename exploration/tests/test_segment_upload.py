"""MCAP 완료와 segment-ref WAL 사이의 크래시 경계 계약."""

from types import SimpleNamespace

import pytest

from fleetsentinel_ingest.segment_upload import SegmentUploader, UploadState
from fleetsentinel_ingest.wal import KIND_SEGMENT_REF, Wal


class _Stub:
    def __init__(self):
        self.complete_calls = 0

    def Complete(self, request):
        self.complete_calls += 1
        return SimpleNamespace(
            verified=True,
            blob_uri=f"s3://bucket/{request.segment_id}.mcap",
        )


def _state(**values):
    defaults = dict(
        segment_id="segment-1",
        upload_id="upload-1",
        blob_uri="s3://bucket/pending.mcap",
        mcap_path="unused.mcap",
        size_bytes=3,
        part_size=8,
        sha256="a" * 64,
        t_start_us=1,
        parts={1: "etag-1"},
    )
    defaults.update(values)
    return UploadState(**defaults)


def test_complete_keeps_recovery_state_until_reference_is_durable(tmp_path):
    wal = Wal(tmp_path / "wal")
    stub = _Stub()
    uploader = SegmentUploader(stub, wal, tmp_path / "wal", part_size=8)
    uploader._save(_state())

    completed = uploader.resume(uploader.pending()[0])

    assert completed.object_verified is True
    assert len(uploader.pending()) == 1
    assert uploader.pending()[0].object_verified is True
    assert stub.complete_calls == 1

    # 완료 상태로 재시작하면 multipart Complete를 다시 호출하지 않는다.
    uploader.resume(uploader.pending()[0])
    assert stub.complete_calls == 1
    wal.close()


def test_reference_fsync_precedes_state_delete_and_is_deduplicated(tmp_path):
    wal_dir = tmp_path / "wal"
    wal = Wal(wal_dir)
    uploader = SegmentUploader(_Stub(), wal, wal_dir, part_size=8)
    completed = _state(object_verified=True)
    uploader._save(completed)
    payload = b"stable-segment-reference"

    # append+fsync 뒤 상태 삭제 전에 죽은 경계를 재현한다.
    assert wal.append(payload, kind=KIND_SEGMENT_REF) == 0
    wal.flush()
    wal.close()

    recovered_wal = Wal(wal_dir)
    recovered = SegmentUploader(_Stub(), recovered_wal, wal_dir, part_size=8)
    seq = recovered.persist_reference(recovered.pending()[0], payload)

    assert seq == 0
    assert recovered_wal.next_seq == 1
    assert [(r.seq, r.kind, r.payload) for r in recovered_wal.read_from(0)] == [
        (0, KIND_SEGMENT_REF, payload)
    ]
    assert recovered.pending() == []
    recovered_wal.close()


def test_unverified_object_cannot_drop_recovery_state(tmp_path):
    wal = Wal(tmp_path / "wal")
    uploader = SegmentUploader(_Stub(), wal, tmp_path / "wal", part_size=8)
    state = _state(object_verified=False)
    uploader._save(state)

    with pytest.raises(RuntimeError, match="검증되지 않은 객체"):
        uploader.persist_reference(state, b"ref")

    assert len(uploader.pending()) == 1
    assert wal.next_seq == 0
    wal.close()
