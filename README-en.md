# FleetSentinel

[한국어](README.md) | [English](README-en.md)

An autonomous-driving platform for ingesting vehicle signals and sensor files and viewing abnormal
states in a fleet console.

![Java](https://img.shields.io/badge/Java_21-ED8B00?style=flat-square&logo=openjdk&logoColor=white)
![Python](https://img.shields.io/badge/Python_3.12-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=flat-square&logo=apacheflink&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![Spring Boot](https://img.shields.io/badge/Spring_Boot_4-6DB33F?style=flat-square&logo=springboot&logoColor=white)
![React](https://img.shields.io/badge/React-61DAFB?style=flat-square&logo=react&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

Docs (Korean): [System Design](docs/sdd.md) · [Data Design](docs/data-design.md) · [MQTT Migration](docs/mqtt-migration.md) · [MQTT Collection Contract](docs/mqtt-contract.md)

## Background

This project started from the real-time speeding detection work in [AutoNotify](https://github.com/Qualcomm-Capstone),
a Qualcomm-sponsored capstone. Feedback from the final presentation was that scaling a single-vehicle
demo to a fleet requires separate designs for ingestion, storage, and operations.

## The Problem

Data from a single autonomous vehicle includes **signals** (CAN, IMU, steering), **object metadata**
(3D boxes, tracks), and **raw sensors** (6 cameras, LiDAR, 5 radars).

These data types have different transfer sizes and message rates, so they use separate paths.

1. Raw sensor data uses 58× more bandwidth than signals, while signals produce 8× more messages.
2. Continuous raw uploads increase network and cloud costs; event- and request-based uploads limit the selected data.

The input data was collected from **2 vehicles**, and the longest continuous stretch is about
20 seconds. In this repository, **"N vehicles" means N concurrent streams**.

Measured figures (rates, sizes, formats, and per-channel detail) are listed in [Data Design](docs/data-design.md).

## Architecture

The current path is selective collection → SQLite outbox → MQTT → bridge → Kafka.
MQTT also carries collection policies and command results. Existing gRPC ingestion and raw-upload control remain available.
See [System Design](docs/sdd.md) for the current architecture. Cloud HA and fleet-scale capacity are not yet validated.

Signals and object metadata go through Kafka and Flink. Raw sensor files go to object storage, with
references recorded in Kafka. ClickHouse's clip catalog connects the two paths.

## Key Design Decisions

| Problem | Solution |
|---|---|
| 58× bandwidth asymmetry | **Claim-Check** — references on the bus, payloads in storage |
| Continuous raw-upload cost | **Event/request-based clips** — file upload is implemented; real-vehicle triggers and automatic retention remain open |
| Recovery after an interrupted upload | **MQTT + SQLite outbox + application ACK**, with legacy gRPC WAL/CACK retained |
| Sensor rates span hundreds-fold | **Three timestamps** + keyframe synchronization anchor |
| Coordinates are not lat/lon | **Official-origin ENU→WGS84 conversion** |
| Raw logs must replay standalone | **MCAP with embedded calibration** |
| Duplicate delivery and gap detection | **Kafka storage acknowledgement + stable record identity and deduplication** ([design](docs/sdd.md)) |
| ODD exits must be visible immediately | **Flink keyed state/timer → Kafka alert → API SSE** |
| Operators need the source around an alert | **Clip catalog `blob_uri` → object-storage replay** |

## Stack

| Layer | Technology | Role |
|---|---|---|
| Vehicle communication | MQTT 5 · SQLite | Selective reporting, commands and durable unacknowledged uploads |
| Ingest and buffering | Apache Kafka | Buffers vehicle records, fans them out to processing and storage, and supports replay |
| Stream processing | Apache Flink | Maintains per-vehicle state for deduplication, validation, coordinate conversion, and ODD detection |
| Raw storage | Object storage + MCAP | Stores large sensor clips as files for later replay |
| Time-series storage | ClickHouse | Stores and queries signals, object metadata, and the clip catalog |
| API | Spring Boot | Exposes ClickHouse queries and Kafka alerts through REST and SSE |
| Console | React · MapLibre GL · uPlot · Rerun | Shows vehicle positions, signals, alerts, and sensor clips |

Signals and object metadata go through Kafka, Flink, and ClickHouse. Raw sensor files go to object
storage. The two paths are connected by the clip ID and `blob_uri`.

## Known Limitations

Current scope and limitations are listed in [SDD §4.2](docs/sdd.md).

- **Not live monitoring.** nuScenes replay reproduces bandwidth, cadence, and format, but there is no real-vehicle integration.
- **Recovery is locally tested.** Legacy gRPC WAL and MQTT SQLite outbox have separate contracts. Real-device power loss, disk failures and inputs not yet durably accepted need additional validation.
- **The console uses a mock stream by default.** `make dashboard` connects it to the real API, but Kafka→Flink→API latency and recovery have not been measured end to end.
- **The source is 2 vehicles with a 20-second continuity ceiling.** "N vehicles" means N concurrent streams, not N distinct real vehicles.
- **Cloud HA is unverified.** Kafka broker-level failover was tested on one host; MQTT uses a single local broker. Host/zone failures and mass reconnection remain open.
- **3D boxes come from nuScenes annotations.** They are not produced by the vehicle and are used only to show surrounding objects in the dashboard.
- **Radar payloads unparsed** — `.pcd` files are stored in MCAP but not decoded or visualized.

## Layout

```
FleetSentinel/
├── frontend/         # (React) ops console — map, time series, clip search, sensor replay
├── exploration/      # (Python) measurement/verification tools + vehicle-side loss prevention (WAL, ack, dedup)
├── flink-pipeline/   # (Java) dedup, validation, coordinate derivation, ODD detection
├── infra/            # docker-compose (Kafka ×3, Flink, ClickHouse, MinIO)
├── schemas/          # canonical Avro schemas
├── scripts/          # infra smoke tests · Kafka HA demo
└── docs/             # design documents
```

## Validation

With JDK 21 and 17 installed, run `./gradlew clean build` at the repository root to test and
package all three Java modules, including gateway integration tests. Gradle is pinned by the Wrapper.
API/gateway use Java 21; Flink uses Java 17. JARs are generated under each module's `build/libs/`.

Use `make mqtt-test` for collection policy, durability, ACK and command tests,
`make mqtt-integration-test` for real MQTT/Kafka/ClickHouse integration,
and `make flink-test` for Flink regressions.

Python dependencies and installation constraints are listed in [exploration](exploration/README.md).
See [SDD §6](docs/sdd.md) for validation scope and limitations.

## License

[MIT License](LICENSE). nuScenes data is subject to Motional's non-commercial terms and is not included in this repository.
