import pytest

from fleetsentinel_ingest.legacy_upload_guard import require_drained_legacy_uploads
from fleetsentinel_ingest.wal import KIND_SEGMENT_REF, Wal


def test_given_missing_wal_when_cutover_checked_then_no_directory_created(tmp_path):
    root = tmp_path / "missing"
    require_drained_legacy_uploads(root)
    assert not root.exists()


@pytest.mark.parametrize("acknowledged", [False, True])
def test_given_legacy_reference_when_cutover_checked_then_require_ack(tmp_path, acknowledged):
    wal = Wal(tmp_path)
    seq = wal.append(b"segment-reference", KIND_SEGMENT_REF)
    wal.flush()
    if acknowledged:
        wal.commit(seq)
    wal.close()
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    if acknowledged:
        require_drained_legacy_uploads(tmp_path)
    else:
        with pytest.raises(RuntimeError, match="unacknowledged"):
            require_drained_legacy_uploads(tmp_path)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}


@pytest.mark.parametrize("filename,data,reason", [
    ("uploads/job.json", b"{}", "intents"),
    ("uploads/job.json.tmp", b"partial", "intents"),
    ("COMMIT", b"broken", "commit pointer"),
    ("00000000000000000000.seg", b"partial", "truncated"),
])
def test_given_pending_or_corrupt_state_when_cutover_checked_then_preserve_and_block(
    tmp_path, filename, data, reason
):
    path = tmp_path / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    with pytest.raises(RuntimeError, match=reason):
        require_drained_legacy_uploads(tmp_path)
    assert path.read_bytes() == data
