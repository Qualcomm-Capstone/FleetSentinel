#!/usr/bin/env bash
# Isolated development PKI. Refuses to overwrite any existing directory/certificates.
set -euo pipefail
if [ "$#" -lt 2 ]; then
  echo "Usage: bash scripts/gen-mqtt-certs.sh NEW_DIRECTORY VEHICLE_ID [VEHICLE_ID...]" >&2
  exit 2
fi
target=$1
shift
if [ -e "$target" ]; then
  echo "Refusing to overwrite existing PKI: $target" >&2
  exit 2
fi
for vehicle in "$@"; do
  if [[ ! "$vehicle" =~ ^vehicle-[a-zA-Z0-9_-]{1,64}$ ]]; then
    echo "Development vehicle CN must start with vehicle-: $vehicle" >&2
    exit 2
  fi
done
root=$(cd "$(dirname "$0")/.." && pwd)
umask 077
mkdir -m 700 -p "$target"
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj /CN=FleetSentinel-MQTT-Dev-CA \
  -keyout "$target/ca.key" -out "$target/ca.crt" >/dev/null 2>&1
for name in server bridge command-service "$@"; do
  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=$name" \
    -keyout "$target/$name.key" -out "$target/$name.csr" >/dev/null 2>&1
  extension="$root/infra/mqtt/client.ext"
  if [ "$name" = server ]; then extension="$root/infra/mqtt/server.ext"; fi
  openssl x509 -req -in "$target/$name.csr" -CA "$target/ca.crt" -CAkey "$target/ca.key" \
    -CAcreateserial -days 7 -extfile "$extension" -out "$target/$name.crt" >/dev/null 2>&1
done
echo "Development MQTT certificates created in $target (7-day leaf validity)."
