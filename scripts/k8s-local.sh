#!/usr/bin/env bash
# Dedicated local cluster; never follows or changes the user's current kubectl context.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
state="$root/mqtt-local/k8s"
kind_bin="${KIND_BIN:-kind}"
cluster=fleetsentinel
context=kind-fleetsentinel
kubeconfig="$state/kubeconfig"
k=(kubectl --kubeconfig "$kubeconfig" --context "$context")
if [ "$#" -gt 0 ]; then
  echo "Usage: bash scripts/k8s-local.sh" >&2
  exit 2
fi

require_cluster() {
  if [ ! -f "$state/owned-cluster" ] || [ ! -f "$kubeconfig" ]; then
    echo 'Run scripts/k8s-local.sh first; no existing context will be adopted.' >&2
    exit 1
  fi
}
secret() {
  local name=$1; shift
  "${k[@]}" -n fleetsentinel create secret generic "$name" "$@" --dry-run=client -o yaml |
    "${k[@]}" -n fleetsentinel apply -f -
}
command -v "$kind_bin" >/dev/null
command -v docker >/dev/null
umask 077
mkdir -p "$state"
if "$kind_bin" get clusters 2>/dev/null | grep -qx "$cluster"; then
  require_cluster
else
  touch "$state/owned-cluster"
  "$kind_bin" create cluster --name "$cluster" --config k8s/kind.yaml --kubeconfig "$kubeconfig" --wait 120s
fi
if [ ! -e "$state/mqtt-pki" ]; then
  bash scripts/gen-mqtt-certs.sh "$state/mqtt-pki" vehicle-0001 vehicle-0002
fi
if [ ! -e "$state/pki" ]; then
  PKI_DIR="$state/pki" bash scripts/gen-certs.sh vehicle-0001 vehicle-0002
fi
for certificate in "$state/mqtt-pki/server.crt" "$state/mqtt-pki/clip-service.crt" \
                   "$state/mqtt-pki/clip-service-2.crt" "$state/pki/server/server.crt"; do
  openssl x509 -checkend 0 -noout -in "$certificate" >/dev/null || {
    echo 'Local certificate expired; renew the local PKI before applying.' >&2; exit 1;
  }
done
./gradlew :gateway:bootJar --console=plain
docker build -t fleetsentinel/gateway:0.1.0 gateway
docker build -f infra/mqtt-bridge.Dockerfile -t fleetsentinel/mqtt-bridge:0.1.0 .
"$kind_bin" load docker-image --name "$cluster" fleetsentinel/gateway:0.1.0 fleetsentinel/mqtt-bridge:0.1.0
# Reuse available local images; missing images are pulled by Kubernetes.
for image in eclipse-mosquitto:2.0.22 nginx:1.30.6-alpine postgres:17.11-alpine \
             apache/kafka:4.0.0 minio/minio:RELEASE.2025-04-08T15-41-24Z minio/mc:RELEASE.2025-04-08T15-39-49Z; do
  if docker image inspect "$image" >/dev/null 2>&1; then
    "$kind_bin" load docker-image --name "$cluster" "$image"
  fi
done
"${k[@]}" apply -f k8s/base/namespace.yaml
secret gateway-pki --from-file=ca.crt="$state/pki/ca/ca.crt" \
  --from-file=server.crt="$state/pki/server/server.crt" --from-file=server.key="$state/pki/server/server.key"
secret object-server-pki --from-file=tls.crt="$state/pki/server/server.crt" --from-file=tls.key="$state/pki/server/server.key"
secret mqtt-server-pki --from-file=ca.crt="$state/mqtt-pki/ca.crt" \
  --from-file=server.crt="$state/mqtt-pki/server.crt" --from-file=server.key="$state/mqtt-pki/server.key"
for identity in clip-service clip-service-2 bridge; do
  name="$identity-pki"
  if [ "$identity" = bridge ]; then name=mqtt-bridge-pki; fi
  secret "$name" --from-file=ca.crt="$state/mqtt-pki/ca.crt" \
    --from-file="$identity.crt=$state/mqtt-pki/$identity.crt" --from-file="$identity.key=$state/mqtt-pki/$identity.key"
done
"${k[@]}" apply -k k8s/overlays/clips-local
# Fixed local image tags and mounted certificates require a fresh process on subsequent runs.
"${k[@]}" -n fleetsentinel rollout restart statefulset/mqtt deployment/gateway deployment/gateway-secondary deployment/mqtt-bridge deployment/minio-tls
for workload in statefulset/mqtt statefulset/postgres statefulset/kafka statefulset/minio \
                deployment/minio-tls deployment/gateway deployment/gateway-secondary deployment/mqtt-bridge; do
  "${k[@]}" -n fleetsentinel rollout status "$workload" --timeout=240s
done
"${k[@]}" -n fleetsentinel wait --for=condition=complete job/minio-init job/kafka-topics --timeout=180s
echo 'Ready: MQTT TLS localhost:30883, signed HTTPS upload https://localhost:30443'
