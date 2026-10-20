"""중량 경로 클라이언트 — MCAP 클립을 오브젝트 스토리지로 올린다.

## 데이터는 게이트웨이를 거치지 않는다

게이트웨이는 **presigned URL만 발급**하고, 298~341 MiB의 MCAP은 차량에서 오브젝트
스토리지로 직행한다. 게이트웨이를 데이터 경로에 넣으면 100대에서 2.5 GiB/s가 통과해
stateless 확장 이점이 사라진다 — 게이트웨이를 프록시로 두는 안을 기각한 이유다.

## 고아 파일을 만들지 않는다

```
1. segment_id 발급 → **의도 파일**을 fsync   ← durable. WAL이 아니다
2. multipart 업로드 (파트마다 의도 파일 갱신)
3. 완료 확인 → 완료 상태를 의도 파일에 fsync
4. KIND_SEGMENT_REF를 WAL에 append + fsync → 의도 파일 삭제
5. 전송기가 발행 → CACK → 커밋
```

1이 durable하므로 **어디서 죽든 재시작 후 미완결 `segment_id`를 열거할 수 있다**.
고아가 안 생기는 게 아니라, 생겨도 반드시 발견되는 것으로 성질이 바뀐다.

## ⚠️ 의도를 WAL에 넣지 않는 이유

설계 초안은 `KIND_SEGMENT_INTENT`를 WAL에 append 하려 했다. **그러면 안 된다.**

WAL의 `seq`는 차량별로 빈틈없이 이어져야 하고, 하류는 **결번을 곧 유실로 판정한다**
([데이터 설계](../../docs/data-design.md) §5.0). 의도 레코드는 게이트웨이로 나가지 않으므로
그 `seq`가 저장 계층에 영영 도착하지 않고, dedup은 그 자리를 **유실로 읽는다.**
로컬 전용 레코드와 발행 대상이 같은 `seq` 공간을 쓰면 핵심 불변식이 깨진다.

그래서 의도는 **별도 파일**(`uploads/<segment_id>.json`)로 남긴다. 어차피 재개에 필요한
`(upload_id, 완료 파트)`를 durable하게 들고 있어야 하므로, 그 파일이 의도 로그를 겸한다 —
파일 하나가 두 역할을 한다.

## 재개

`(segment_id, upload_id, 완료 파트)`를 WAL과 같은 디스크에 둔다. 프로세스가 죽어도
재시작 후 이미 올라간 파트를 다시 보내지 않는다.
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from .wal import KIND_SEGMENT_REF

__all__ = [
    "DEFAULT_PART_SIZE",
    "UploadState",
    "SegmentUploader",
    "sha256_of",
]

#: 파트 크기. 341 MiB 클립 → 22파트. S3 최소가 5 MiB이고, 크면 재전송 손실이 크고
#: 작으면 요청 수가 는다.
DEFAULT_PART_SIZE = 16 * 1024 * 1024

#: 업로드 상태 파일 이름. WAL과 같은 디렉터리에 둔다 — 같이 살아남아야 재개가 성립한다.
STATE_DIR = "uploads"


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    """파일 전체의 sha256(hex).

    ETag를 쓰지 않는 이유는 multipart ETag가 `MD5(MD5(part1)+…)-N` 형식이라
    **콘텐츠 해시가 아니기** 때문이다 — 파트 크기가 바뀌면 같은 내용이 다른 값을 갖는다.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


@dataclass
class UploadState:
    """재개에 필요한 최소 상태. 이걸 잃으면 처음부터 다시 올려야 한다."""

    segment_id: str
    upload_id: str
    blob_uri: str
    mcap_path: str
    size_bytes: int
    part_size: int
    sha256: str
    #: 키 재구성에 필요하다 — 게이트웨이가 (vehicle_id, segment_id, t_start)로 키를
    #: 만든다. 재시작 후에도 알아야 하므로 상태에 남긴다.
    t_start_us: int = 0
    #: 완료된 파트. `{part_number: etag}`
    parts: dict = field(default_factory=dict)
    #: Complete 응답과 객체 SHA-256 검증까지 끝났는지 여부. 참조 WAL 기록 전에는
    #: 상태 파일을 지우지 않으므로, 이 값으로 재시작 시 Complete를 반복하지 않는다.
    object_verified: bool = False

    def to_json(self) -> str:
        d = dict(self.__dict__)
        d["parts"] = {str(k): v for k, v in self.parts.items()}
        return json.dumps(d, ensure_ascii=False)

    @staticmethod
    def from_json(raw: str) -> "UploadState":
        d = json.loads(raw)
        d["parts"] = {int(k): v for k, v in d.get("parts", {}).items()}
        # 수정 전 상태 파일도 재개할 수 있어야 한다. 이 경우 Complete를 한 번 더 호출하고,
        # 서버의 멱등 처리로 이미 완성된 객체를 확인한다.
        d.setdefault("object_verified", False)
        return UploadState(**d)

    @property
    def part_count(self) -> int:
        return max(1, (self.size_bytes + self.part_size - 1) // self.part_size)

    def missing_parts(self) -> List[int]:
        return [n for n in range(1, self.part_count + 1) if n not in self.parts]


class SegmentUploader:
    """MCAP 하나를 올린다.

    :param stub: `SegmentUploadStub` (gRPC). 제어 평면만 쓴다
    :param wal: 의도·참조를 남길 WAL
    :param state_root: 업로드 상태 파일 위치. 보통 WAL과 같은 디렉터리
    """

    def __init__(self, stub, wal, state_root: Path | str,
                 part_size: int = DEFAULT_PART_SIZE) -> None:
        self._stub = stub
        self._wal = wal
        self._state_dir = Path(state_root) / STATE_DIR
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._part_size = part_size

    # ── 상태 영속화 ──────────────────────────────────────────────────────

    def _state_path(self, segment_id: str) -> Path:
        return self._state_dir / f"{segment_id}.json"

    def _save(self, st: UploadState) -> None:
        """원자적 교체. 쓰다 죽으면 이전 상태가 남아야 한다."""
        p = self._state_path(st.segment_id)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(st.to_json())
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        tmp.replace(p)
        self._fsync_state_dir()

    def _load(self, segment_id: str) -> Optional[UploadState]:
        p = self._state_path(segment_id)
        if not p.exists():
            return None
        return UploadState.from_json(p.read_text())

    def pending(self) -> List[UploadState]:
        """완료되지 않은 업로드. 재시작 후 이걸로 이어간다."""
        out = []
        for p in sorted(self._state_dir.glob("*.json")):
            try:
                out.append(UploadState.from_json(p.read_text()))
            except (json.JSONDecodeError, TypeError):
                continue  # 쓰다 죽어 깨진 파일 — 다음 begin이 새로 만든다
        return out

    def _discard(self, segment_id: str) -> None:
        path = self._state_path(segment_id)
        if path.exists():
            path.unlink()
            self._fsync_state_dir()

    def _fsync_state_dir(self) -> None:
        """rename/unlink 자체도 전원 손실 뒤 남도록 디렉터리를 동기화한다."""
        fd = os.open(self._state_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ── 업로드 ───────────────────────────────────────────────────────────

    def upload(
        self,
        mcap_path: Path | str,
        t_start_us: int,
        t_end_us: int,
        sensor_channels: Sequence[str],
        sample_count: int,
        scene_id: str = "",
        on_intent: Optional[Callable[[UploadState], None]] = None,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> UploadState:
        """클립 하나를 끝까지 올린다.

        :param on_intent: 의도 파일이 fsync된 직후 불린다. 관측용 훅이며 durability는
            이미 확보돼 있다
        :returns: 완료된 상태. `blob_uri`가 확정돼 있다
        """
        from .proto import ingest_pb2

        path = Path(mcap_path)
        size = path.stat().st_size
        digest = sha256_of(path)

        resp = self._stub.Begin(ingest_pb2.BeginUploadRequest(
            t_start_us=t_start_us,
            t_end_us=t_end_us,
            size_bytes=size,
            sha256=digest,
            part_size_bytes=self._part_size,
            scene_id=scene_id,
            sensor_channels=list(sensor_channels),
            sample_count=sample_count,
        ))

        st = UploadState(
            segment_id=resp.segment_id,
            upload_id=resp.upload_id,
            blob_uri=resp.blob_uri,
            mcap_path=str(path),
            size_bytes=size,
            part_size=self._part_size,
            sha256=digest,
            t_start_us=t_start_us,
        )
        # ① 의도를 먼저 durable하게. 여기서 죽어도 pending()이 찾아낸다.
        #    WAL이 아니라 별도 파일인 이유는 모듈 독스트링 참조.
        self._save(st)
        if on_intent is not None:
            on_intent(st)

        self._put_parts(st, {u.part_number: u.url for u in resp.part_urls}, on_progress)
        return self._complete(st)

    def resume(self, st: UploadState,
               on_progress: Optional[Callable[[int, int], None]] = None) -> UploadState:
        """미완결 업로드를 이어간다. **이미 올라간 파트는 다시 보내지 않는다.**

        URL은 만료됐을 수 있으므로 `Refresh`로 다시 발급받는다. 게이트웨이는 상태를 갖지
        않으므로 같은 `upload_id`로 서명만 새로 해준다.
        """
        from .proto import ingest_pb2

        if st.object_verified:
            return st

        missing = st.missing_parts()
        if not missing:
            return self._complete(st)

        resp = self._stub.Refresh(ingest_pb2.RefreshUrlsRequest(
            segment_id=st.segment_id,
            upload_id=st.upload_id,
            part_numbers=missing,
            t_start_us=st.t_start_us,
        ))
        self._put_parts(st, {u.part_number: u.url for u in resp.part_urls}, on_progress)
        return self._complete(st)

    def persist_reference(self, st: UploadState, payload: bytes) -> int:
        """검증된 객체의 참조를 WAL에 durable하게 남기고 재개 상태를 정리한다.

        WAL append 뒤 상태 파일 삭제 전에 죽으면 재시작 시 같은 payload를 찾는다. 이 경우
        새 seq를 만들지 않고 기존 레코드를 재사용한다. 이 순서 덕분에 Kafka가 실패해도
        참조는 WAL에 남고, 객체만 존재한 채 발견할 수 없는 상태가 되지 않는다.
        """
        if not st.object_verified:
            raise RuntimeError(f"검증되지 않은 객체의 참조는 기록할 수 없다: {st.segment_id}")

        start = max(0, self._wal.committed_seq + 1)
        seq = None
        for record in self._wal.read_from(start):
            if record.kind == KIND_SEGMENT_REF and record.payload == payload:
                seq = record.seq
                break
        if seq is None:
            seq = self._wal.append(payload, kind=KIND_SEGMENT_REF)

        # 상태 삭제보다 WAL fsync가 반드시 먼저다. Kafka 발행/CACK은 그 다음 단계다.
        self._wal.flush()
        self._discard(st.segment_id)
        return seq

    # ── 내부 ─────────────────────────────────────────────────────────────

    def _put_parts(self, st: UploadState, urls: dict,
                   on_progress: Optional[Callable[[int, int], None]]) -> None:
        total = st.part_count
        with open(st.mcap_path, "rb") as f:
            for n in range(1, total + 1):
                if n in st.parts:
                    continue  # 이미 올렸다
                f.seek((n - 1) * st.part_size)
                body = f.read(st.part_size)
                url = urls.get(n)
                if url is None:
                    raise RuntimeError(f"파트 {n}의 presigned URL이 없다")
                etag = _put(url, body)
                st.parts[n] = etag
                self._save(st)      # 파트마다 저장 — 죽어도 여기까지는 재사용된다
                if on_progress is not None:
                    on_progress(n, total)

    def _complete(self, st: UploadState) -> UploadState:
        from .proto import ingest_pb2

        if st.object_verified:
            return st

        parts = [ingest_pb2.PartEtag(part_number=n, etag=st.parts[n])
                 for n in sorted(st.parts)]
        resp = self._stub.Complete(ingest_pb2.CompleteUploadRequest(
            segment_id=st.segment_id, upload_id=st.upload_id, parts=parts,
            t_start_us=st.t_start_us))
        if not resp.verified:
            raise RuntimeError(f"게이트웨이가 무결성을 확인하지 못했다: {st.segment_id}")
        st.blob_uri = resp.blob_uri
        st.object_verified = True
        # 아직 ref가 WAL에 없으므로 상태를 지우면 안 된다. 완료 사실을 먼저 durable하게
        # 남기고 persist_reference()가 WAL fsync 뒤 정리한다.
        self._save(st)
        return st

    def abort(self, st: UploadState) -> bool:
        from .proto import ingest_pb2

        resp = self._stub.Abort(ingest_pb2.AbortUploadRequest(
            segment_id=st.segment_id, upload_id=st.upload_id,
            t_start_us=st.t_start_us))
        self._discard(st.segment_id)
        return resp.aborted


def _put(url: str, body: bytes) -> str:
    """presigned URL로 파트를 올리고 ETag를 돌려준다.

    `requests` 대신 stdlib을 쓴다 — 차량 측 의존성을 늘리지 않는다.
    """
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Content-Length", str(len(body)))
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            etag = r.headers.get("ETag")
            if not etag:
                raise RuntimeError("ETag가 응답에 없다 — multipart 완료에 필요하다")
            return etag.strip('"')
    except urllib.error.HTTPError as e:
        detail = e.read()[:400].decode("utf-8", "replace")
        raise RuntimeError(f"파트 업로드 실패 HTTP {e.code}: {detail}") from e
