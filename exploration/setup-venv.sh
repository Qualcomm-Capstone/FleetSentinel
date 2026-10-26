#!/usr/bin/env bash
# 탐색 환경 구성 — 2단계 설치가 필수다.
#
# nuscenes-devkit 1.2.0은 `numpy<2.0.0`을 선언하지만, rerun-sdk 0.23은 numpy 2의
# asarray(copy=) 시그니처를 요구한다. numpy 1.26에서는 쿼터니언 배치가 **조용히
# 누락**되고 경고만 남아 회전이 빠진 재생본이 만들어진다(실측).
#
# 그래서 devkit을 먼저 설치해 의존성을 해소한 뒤 numpy만 2.x로 덮어쓴다. 한 번에
# 설치하면 pip이 ResolutionImpossible로 거부한다. devkit의 선언은 보수적이며 판독
# 경로가 numpy 2에서 정상 동작함을 변환·검증 게이트로 확인했다.
set -euo pipefail
cd "$(dirname "$0")"

PY="${PY:-python3.12}"
"$PY" -m venv .venv
./.venv/bin/pip install -q --upgrade pip

echo "[1/2] devkit + 도구 설치 (numpy<2가 함께 깔린다)"
./.venv/bin/pip install -q \
  nuscenes-devkit==1.2.0 \
  rerun-sdk==0.23.1 \
  mcap==1.4.0 \
  python-ulid==4.0.1 \
  pytest==9.1.1 \
  grpcio==1.83.0 \
  grpcio-tools==1.83.0 \
  fastavro==1.12.2 \
  confluent-kafka==2.15.0 \
  paho-mqtt==2.1.0

echo "[2/2] numpy를 2.x로 덮어쓰기 (devkit 선언은 의도적으로 무시)"
./.venv/bin/pip install -q -U 'numpy>=2.1'

echo
./.venv/bin/python - <<'PY'
import numpy, mcap, rerun
from nuscenes.nuscenes import NuScenes  # 임포트 가능 여부만 확인
print(f"✅ numpy {numpy.__version__} · rerun {rerun.__version__} · devkit 임포트 OK")
assert numpy.__version__.split(".")[0] == "2", "numpy 2가 아니면 회전이 누락된다"
PY
