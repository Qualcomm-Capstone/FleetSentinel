package io.fleetsentinel.gateway.clip;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.fleetsentinel.gateway.GatewayApplication;
import java.net.URI;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.testcontainers.utility.MountableFile;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/** Real PostgreSQL, MinIO, Kafka and mTLS MQTT control and two application instances. No mocked external service. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClipUploadIT {
    private final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17.11-alpine");
    private final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");
    private final GenericContainer<?> minio = new GenericContainer<>("minio/minio:RELEASE.2025-04-08T15-41-24Z")
            .withEnv("MINIO_ROOT_USER", "testaccess").withEnv("MINIO_ROOT_PASSWORD", "testsecret123")
            .withCommand("server", "/data").withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000));
    private ConfigurableApplicationContext first, second;
    private Path pki, mqttPki;
    private GenericContainer<?> broker;
    private final Map<MqttClient, BlockingQueue<byte[]>> replies = new ConcurrentHashMap<>();
    private MqttClient vehicle1, vehicle2;
    private ClipStore store, other;
    private S3Client s3;
    private final ClipJson json = new ClipJson();
    private static final String VEHICLE = "vehicle-0001";

    @BeforeAll
    void start() throws Exception {
        Startables.deepStart(postgres, kafka, minio).join();
        pki = Files.createTempDirectory("clip-rest-pki-");
        var process = new ProcessBuilder("bash", Path.of("..", "scripts", "gen-certs.sh").toAbsolutePath().toString(),
                VEHICLE, "vehicle-0002").redirectErrorStream(true);
        process.environment().put("PKI_DIR", pki.toString());
        var running = process.start();
        String output = new String(running.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(running.waitFor()).as(output).isZero();
        mqttPki = pki.resolve("mqtt");
        var generate = new ProcessBuilder("bash", Path.of("..", "scripts", "gen-mqtt-certs.sh").toAbsolutePath().toString(),
                mqttPki.toString(), VEHICLE, "vehicle-0002").redirectErrorStream(true).start();
        String mqttOutput = new String(generate.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(generate.waitFor()).as(mqttOutput).isZero();
        broker = new GenericContainer<>("eclipse-mosquitto:2.0.22")
                .withCreateContainerCmdModifier(cmd -> cmd.withUser("0"))
                .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "infra", "mqtt", "mosquitto.conf")), "/mosquitto/config/mosquitto.conf")
                .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "infra", "mqtt", "acl")), "/mosquitto/config/acl")
                .withCopyFileToContainer(MountableFile.forHostPath(mqttPki), "/mosquitto/config/pki")
                .withExposedPorts(8883);
        // Docker-assigned ports can change on restart; recovery assumes a stable broker endpoint.
        try (var port = new java.net.ServerSocket(0)) {
            broker.setPortBindings(List.of("127.0.0.1:" + port.getLocalPort() + ":8883"));
        }
        broker.start();
        first = application();
        second = application();
        store = first.getBean(ClipStore.class);
        other = second.getBean(ClipStore.class);
        s3 = first.getBean(S3Client.class);
        s3.createBucket(b -> b.bucket("clip-test"));
        vehicle1 = mqttClient(VEHICLE);
        vehicle2 = mqttClient("vehicle-0002");
    }

    @AfterAll
    void stop() {
        if (second != null) second.close();
        if (first != null) first.close();
        for (var client : replies.keySet()) {
            try { client.disconnect(); client.close(); } catch (MqttException ignored) { }
        }
        if (broker != null) broker.stop();
        minio.stop(); kafka.stop(); postgres.stop();
    }

    @BeforeEach
    void clearDatabase() {
        if (first != null) first.getBean(JdbcTemplate.class).execute("TRUNCATE clip_upload_outbox, clip_upload_jobs");
    }

    @Test
    void mqttScopesEveryOperationToCertificateBoundTopic() throws Exception {
        assertThatThrownBy(() -> mqttClient(null)).isInstanceOf(MqttException.class);
        var created = call(vehicle1, first, "POST", "", request("tls", "data"));
        String id = tree(created).get("id").asString();
        assertThat(call(vehicle2, first, "GET", "/" + id, null).statusCode()).isEqualTo(404);
        assertThat(call(vehicle2, first, "DELETE", "/" + id, null).statusCode()).isEqualTo(404);
        assertThat(call(vehicle2, first, "POST", "/" + id + "/part-urls", new ClipApi.UrlRequest(List.of(1))).statusCode()).isEqualTo(404);
        // A vehicle cannot write another vehicle's request topic or forge a server receipt.
        replies.get(vehicle2).clear();
        byte[] forged = json.write(Map.of("version", 1, "correlation_id", "forged", "operation", "create",
                "body", request("spoof", "data"))).getBytes(StandardCharsets.UTF_8);
        vehicle1.publish("fleet/v1/vehicle-0002/clip/request", forged, 1, false);
        vehicle1.publish("fleet/v1/vehicle-0002/clip/response", forged, 1, false);
        assertThat(replies.get(vehicle2).poll(500, TimeUnit.MILLISECONDS)).isNull();
        assertThat(first.getBean(JdbcTemplate.class).queryForObject(
                "SELECT count(*) FROM clip_upload_jobs WHERE request_id='spoof'", Integer.class)).isZero();
    }

    @Test
    void lostCreateReplyAndServerReconnectKeepOneJob() throws Exception {
        var request = request("lost-reply", "abc");
        String original = tree(call(vehicle1, first, "POST", "", request)).get("id").asString();
        // The edge persisted its manifest but lost the reply/job ID before local commit.
        first.close(); first = application(); store = first.getBean(ClipStore.class);
        s3 = first.getBean(S3Client.class);
        String replay = tree(call(vehicle1, first, "POST", "", request)).get("id").asString();
        assertThat(replay).isEqualTo(original);
        assertThat(first.getBean(JdbcTemplate.class).queryForObject("SELECT count(*) FROM clip_upload_jobs", Integer.class)).isEqualTo(1);
    }

    @Test
    void brokerRestartAfterCompleteAcceptanceRecoversSameJobAndReference() throws Exception {
        var request = request("broker-restart", "abc");
        String id = tree(call(vehicle1, first, "POST", "", request)).get("id").asString();
        processNext(first);
        ClipJob ready = store.get(VEHICLE, id);
        String etag = s3.uploadPart(b -> b.bucket(ready.bucket).key(ready.objectKey)
                .uploadId(ready.uploadId).partNumber(1), RequestBody.fromString("abc")).eTag();
        var complete = new ClipApi.Complete(ready.uploadId, List.of(new ClipApi.Part(1, etag)));
        assertThat(tree(call(vehicle1, first, "POST", "/" + id + "/complete", complete))
                .get("state").asString()).isEqualTo("VERIFYING");
        // Discard the application reply at the edge and restart the actual broker.
        int originalPort = broker.getMappedPort(8883);
        broker.getDockerClient().restartContainerCmd(broker.getContainerId()).withTimeout(2).exec();
        String restartedPort = broker.getCurrentContainerInfo().getNetworkSettings().getPorts()
                .getBindings().get(com.github.dockerjava.api.model.ExposedPort.tcp(8883))[0].getHostPortSpec();
        assertThat(Integer.parseInt(restartedPort)).as("broker endpoint must survive restart").isEqualTo(originalPort);
        for (MqttClient client : List.of(vehicle1, vehicle2)) {
            await().atMost(Duration.ofSeconds(15)).until(() -> !client.isConnected());
            client.close();
            replies.remove(client);
        }
        vehicle1 = mqttClient(VEHICLE);
        vehicle2 = mqttClient("vehicle-0002");
        // Gateway endpoints must reconnect and restore their shared subscription themselves.
        assertThat(tree(call(vehicle1, first, "POST", "", request)).get("id").asString()).isEqualTo(id);
        assertThat(tree(call(vehicle1, first, "POST", "/" + id + "/complete", complete))
                .get("state").asString()).isEqualTo("VERIFYING");
        processNext(second);
        processNext(first);
        assertThat(tree(call(vehicle1, first, "GET", "/" + id, null))
                .get("state").asString()).isEqualTo("COMPLETED");
        assertThat(recordsFor(id, 1)).hasSize(1);
        assertThat(first.getBean(JdbcTemplate.class).queryForObject(
                "SELECT count(*) FROM clip_upload_jobs", Integer.class)).isEqualTo(1);
    }

    @Test
    void twoInstancesShareUniqueIdempotencyAndLeaseFencing() throws Exception {
        var request = request("racing-create", "abc");
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> store.create(VEHICLE, request));
            var b = executor.submit(() -> other.create(VEHICLE, request));
            assertThat(a.get().id).isEqualTo(b.get().id);
        }
        assertThatThrownBy(() -> other.create(VEHICLE, request("racing-create", "different")))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class)
                .hasMessageContaining("409");
        ClipJob stale = store.claim().orElseThrow();
        assertThat(other.claim()).isEmpty();
        expire(stale);
        ClipJob current = other.claim().orElseThrow();
        assertThat(store.uploading(stale, "stale-upload")).isFalse();
        assertThat(other.uploading(current, "current-upload")).isTrue();
        assertThat(store.get(VEHICLE, current.id).uploadId).isEqualTo("current-upload");
    }

    @Test
    void realMultipartUploadVerifiesThenPublishesAvroBeforeCompleted() throws Exception {
        byte[] data = "real clip bytes".getBytes(StandardCharsets.UTF_8);
        var response = call(vehicle1, first, "POST", "", request("happy", "real clip bytes"));
        String id = tree(response).get("id").asString();
        processNext(first);
        var urls = call(vehicle1, second, "POST", "/" + id + "/part-urls", new ClipApi.UrlRequest(List.of(1)));
        String url = tree(urls).get("part_urls").get(0).get("url").asString();
        var uploaded = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(url))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(uploaded.statusCode()).isEqualTo(200);
        String etag = uploaded.headers().firstValue("etag").orElseThrow();
        var manifest = new ClipApi.Complete(tree(urls).get("upload_id").asString(), List.of(new ClipApi.Part(1, etag)));
        assertThat(call(vehicle1, second, "POST", "/" + id + "/complete", manifest).statusCode()).isEqualTo(200);
        assertThat(store.get(VEHICLE, id).state).isEqualTo(ClipState.VERIFYING);
        assertThat(call(vehicle1, first, "DELETE", "/" + id, null).statusCode()).isEqualTo(409);
        processNext(second);
        var verified = store.get(VEHICLE, id);
        assertThat(verified.state).isEqualTo(ClipState.STORED);
        assertThat(verified.actualSha256).isEqualTo(request("unused", "real clip bytes").sha256());
        assertThat(store.outbox(id).publishedAt).isNull();
        processNext(first);
        assertThat(store.get(VEHICLE, id).state).isEqualTo(ClipState.COMPLETED);
        assertThat(store.outbox(id).publishedAt).isNotNull();
        assertThat(call(vehicle1, first, "POST", "/" + id + "/complete", manifest).statusCode()).isEqualTo(200);
        var records = recordsFor(id, 1);
        assertThat(records).hasSize(1);
        assertThat(header(records.getFirst(), "producer_origin")).isEqualTo("clip-upload");
        assertThat(header(records.getFirst(), "boot_id")).isEqualTo("clip-" + id);
        assertThat(header(records.getFirst(), "seq")).isEqualTo("0");
        var schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schemas/segment-ref.avsc"));
        GenericRecord reference = new GenericDatumReader<GenericRecord>(schema)
                .read(null, DecoderFactory.get().binaryDecoder(records.getFirst().value(), null));
        assertThat(reference.get("segment_id").toString()).isEqualTo(id);
        assertThat(reference.get("checksum").toString()).isEqualTo(verified.actualSha256);
    }

    @Test
    void ambiguousCreateRecoversExactKeyAndAbortsDuplicatesWithoutTouchingNeighbor() {
        ClipJob job = store.create(VEHICLE, request("create-crash", "abc"));
        ClipJob crashed = store.claim().orElseThrow();
        var objects = first.getBean(ClipObjectStore.class);
        String firstId = objects.createOrRecover(crashed); // S3 accepted, process died before DB commit.
        String duplicate = s3.createMultipartUpload(b -> b.bucket(job.bucket).key(job.objectKey)).uploadId();
        String neighborKey = job.objectKey + ".neighbor";
        String neighbor = s3.createMultipartUpload(b -> b.bucket(job.bucket).key(neighborKey)).uploadId();
        expire(crashed);
        processNext(second);
        String adopted = store.get(VEHICLE, job.id).uploadId;
        assertThat(adopted).isIn(firstId, duplicate);
        var active = s3.listMultipartUploads(b -> b.bucket(job.bucket)).uploads();
        assertThat(active).extracting(u -> u.uploadId()).contains(adopted, neighbor);
        assertThat(active.stream().filter(u -> u.key().equals(job.objectKey))).hasSize(1);
        store.cancel(VEHICLE, job.id);
        processNext(second);
        assertThat(store.get(VEHICLE, job.id).state).isEqualTo(ClipState.ABORTED);
        var remaining = s3.listMultipartUploads(b -> b.bucket(job.bucket)).uploads();
        assertThat(remaining).extracting(u -> u.uploadId()).contains(neighbor).doesNotContain(firstId, duplicate);
    }

    @Test
    void completeAndKafkaAckCrashBoundariesReplayWithImmutableIdentity() throws Exception {
        ClipJob job = uploaded("complete-crash", "abc", "abc");
        ClipJob crashedVerification = store.claim().orElseThrow();
        var actual = first.getBean(ClipObjectStore.class).verify(crashedVerification, () -> true);
        assertThat(actual.failure()).isNull(); // Object committed and hashed, DB commit lost.
        expire(crashedVerification);
        processNext(second);
        assertThat(store.get(VEHICLE, job.id).state).isEqualTo(ClipState.STORED);
        ClipJob crashedPublish = store.claim().orElseThrow();
        ClipOutbox outbox = store.outbox(job.id);
        byte[] immutablePayload = outbox.payload.clone();
        sendOutbox(outbox); // Broker ACK was received; DB update lost.
        assertThat(store.get(VEHICLE, job.id).state).isEqualTo(ClipState.STORED);
        expire(crashedPublish);
        processNext(second);
        assertThat(store.get(VEHICLE, job.id).state).isEqualTo(ClipState.COMPLETED);
        var duplicates = recordsFor(job.id, 2);
        assertThat(duplicates).hasSize(2);
        for (var record : duplicates) {
            assertThat(record.value()).isEqualTo(immutablePayload);
            assertThat(header(record, "boot_id")).isEqualTo("clip-" + job.id);
            assertThat(header(record, "producer_origin")).isEqualTo("clip-upload");
        }
    }

    @Test
    void mismatchRetainsActualDigestAndNeverEmitsReference() {
        ClipJob job = uploaded("mismatch", "abc", "xyz");
        processNext(second);
        ClipJob failed = store.get(VEHICLE, job.id);
        assertThat(failed.state).isEqualTo(ClipState.FAILED);
        assertThat(failed.actualSha256).isEqualTo(request("actual", "xyz").sha256());
        assertThat(failed.error).isEqualTo("sha256_mismatch");
        assertThat(store.outbox(job.id)).isNull();
    }

    @Test
    void cancellationAndCompletionRaceHasExactlyOneWinner() throws Exception {
        ClipJob job = store.create(VEHICLE, request("cancel-race", "abc"));
        processNext(first);
        ClipJob ready = store.get(VEHICLE, job.id);
        var etag = s3.uploadPart(b -> b.bucket(ready.bucket).key(ready.objectKey).uploadId(ready.uploadId).partNumber(1),
                RequestBody.fromString("abc")).eTag();
        var gate = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var cancel = executor.submit(() -> race(gate, () -> store.cancel(VEHICLE, job.id)));
            var complete = executor.submit(() -> race(gate,
                    () -> other.complete(VEHICLE, job.id, new ClipApi.Complete(ready.uploadId, List.of(new ClipApi.Part(1, etag))))));
            gate.countDown();
            assertThat(List.of(cancel.get(), complete.get())).containsExactlyInAnyOrder(true, false);
        }
        assertThat(store.get(VEHICLE, job.id).state).isIn(ClipState.ABORTING, ClipState.VERIFYING);
        processNext(first);
        assertThat(store.get(VEHICLE, job.id).state).isIn(ClipState.ABORTED, ClipState.STORED);
    }

    @Test
    void cancelDuringCreatePreservesLeaseUntilOutstandingCreateCanBeReconciled() {
        ClipJob job = store.create(VEHICLE, request("cancel-create", "abc"));
        ClipJob inFlight = store.claim().orElseThrow();
        store.cancel(VEHICLE, job.id);
        assertThat(other.claim()).isEmpty();
        String late = first.getBean(ClipObjectStore.class).createOrRecover(inFlight);
        assertThat(store.uploading(inFlight, late)).isFalse();
        expire(inFlight);
        processNext(second);
        assertThat(store.get(VEHICLE, job.id).state).isEqualTo(ClipState.ABORTED);
        assertThat(s3.listMultipartUploads(b -> b.bucket(job.bucket).prefix(job.objectKey)).uploads()).isEmpty();
    }

    @Test
    void autonomousWorkersAdvanceDurableQueueWithoutRequestThreadProcessing() throws Exception {
        try (var automatic = application(true)) {
            var accepted = call(vehicle1, automatic, "POST", "", request("automatic", "background"));
            assertThat(accepted.statusCode()).isEqualTo(200);
            String id = tree(accepted).get("id").asString();
            await().atMost(Duration.ofSeconds(10)).until(() -> store.get(VEHICLE, id).state == ClipState.UPLOADING);
            ClipJob ready = store.get(VEHICLE, id);
            var etag = s3.uploadPart(b -> b.bucket(ready.bucket).key(ready.objectKey)
                    .uploadId(ready.uploadId).partNumber(1), RequestBody.fromString("background")).eTag();
            assertThat(call(vehicle1, automatic, "POST", "/" + id + "/complete",
                    new ClipApi.Complete(ready.uploadId, List.of(new ClipApi.Part(1, etag)))).statusCode()).isEqualTo(200);
            await().atMost(Duration.ofSeconds(15)).until(() -> store.get(VEHICLE, id).state == ClipState.COMPLETED);
            assertThat(store.outbox(id).publishedAt).isNotNull();
        }
    }

    @Test
    void multipartManifestCannotSkipDuplicateOrChangeAlreadyAcceptedParts() {
        byte[] bytes = new byte[5 * 1024 * 1024 + 3];
        new Random(1).nextBytes(bytes);
        var request = new ClipApi.Create("multipart", 1, 2, bytes.length,
                HexFormat.of().formatHex(ClipJson.digest().digest(bytes)), 5 * 1024 * 1024,
                null, List.of("lidar"), 1, Map.of());
        ClipJob job = store.create(VEHICLE, request);
        processNext(first);
        ClipJob ready = store.get(VEHICLE, job.id);
        var parts = new ArrayList<ClipApi.Part>();
        for (int number = 1; number <= 2; number++) {
            int n = number;
            byte[] part = Arrays.copyOfRange(bytes, (number - 1) * 5 * 1024 * 1024,
                    Math.min(number * 5 * 1024 * 1024, bytes.length));
            var etag = s3.uploadPart(b -> b.bucket(ready.bucket).key(ready.objectKey).uploadId(ready.uploadId).partNumber(n),
                    RequestBody.fromBytes(part)).eTag();
            parts.add(new ClipApi.Part(number, etag));
        }
        assertThatThrownBy(() -> store.complete(VEHICLE, job.id, new ClipApi.Complete(ready.uploadId, List.of(parts.getFirst()))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.complete(VEHICLE, job.id,
                new ClipApi.Complete(ready.uploadId, List.of(parts.getFirst(), parts.getFirst())))).isInstanceOf(IllegalArgumentException.class);
        store.complete(VEHICLE, job.id, new ClipApi.Complete(ready.uploadId, parts.reversed()));
        other.complete(VEHICLE, job.id, new ClipApi.Complete(ready.uploadId, parts));
        assertThatThrownBy(() -> other.complete(VEHICLE, job.id,
                new ClipApi.Complete(ready.uploadId, List.of(new ClipApi.Part(1, "different"), parts.getLast()))))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class).hasMessageContaining("409");
        processNext(second);
        assertThat(store.get(VEHICLE, job.id).state).isEqualTo(ClipState.STORED);
        assertThat(store.get(VEHICLE, job.id).actualSizeBytes).isEqualTo(bytes.length);
    }

    @Test
    void expiredMultipartRestartsUploadingGenerationAndRejectsOldCompletion() {
        ClipJob job = store.create(VEHICLE, request("expired-uploading", "abc"));
        processNext(first);
        ClipJob old = store.get(VEHICLE, job.id);
        s3.abortMultipartUpload(b -> b.bucket(old.bucket).key(old.objectKey).uploadId(old.uploadId));
        first.getBean(JdbcTemplate.class).update("UPDATE clip_upload_jobs SET next_attempt=clock_timestamp() WHERE id=?", job.id);
        processNext(second);
        ClipJob restarted = store.get(VEHICLE, job.id);
        assertThat(restarted.state).isEqualTo(ClipState.UPLOADING);
        assertThat(restarted.uploadId).isNotEqualTo(old.uploadId);
        assertThatThrownBy(() -> store.complete(VEHICLE, job.id,
                new ClipApi.Complete(old.uploadId, List.of(new ClipApi.Part(1, "stale")))))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class).hasMessageContaining("409");
        finishRestarted(restarted);
    }

    @Test
    void expiredMultipartDuringVerificationReturnsToUploadingWithFreshGeneration() {
        ClipJob old = uploaded("expired-verifying", "abc", "abc");
        s3.abortMultipartUpload(b -> b.bucket(old.bucket).key(old.objectKey).uploadId(old.uploadId));
        processNext(second);
        ClipJob restarted = store.get(VEHICLE, old.id);
        assertThat(restarted.state).isEqualTo(ClipState.UPLOADING);
        assertThat(restarted.uploadId).isNotEqualTo(old.uploadId);
        assertThat(restarted.completeParts).isNull();
        assertThat(store.outbox(old.id)).isNull();
        finishRestarted(restarted);
    }

    private void finishRestarted(ClipJob restarted) {
        var etag = s3.uploadPart(b -> b.bucket(restarted.bucket).key(restarted.objectKey)
                .uploadId(restarted.uploadId).partNumber(1), RequestBody.fromString("abc")).eTag();
        store.complete(VEHICLE, restarted.id, new ClipApi.Complete(restarted.uploadId, List.of(new ClipApi.Part(1, etag))));
        processNext(first);
        processNext(second);
        assertThat(store.get(VEHICLE, restarted.id).state).isEqualTo(ClipState.COMPLETED);
        assertThat(ClipJson.MAPPER.readTree(store.outbox(restarted.id).headers).get("boot_id").asString())
                .isEqualTo("clip-" + restarted.id);
    }

    private boolean race(CountDownLatch gate, Callable<ClipJob> action) throws Exception {
        gate.await();
        try { action.call(); return true; }
        catch (org.springframework.web.server.ResponseStatusException e) {
            assertThat(e.getStatusCode().value()).isEqualTo(409); return false;
        }
    }

    private ClipJob uploaded(String requestId, String expected, String content) {
        ClipJob job = store.create(VEHICLE, request(requestId, expected));
        processNext(first);
        ClipJob ready = store.get(VEHICLE, job.id);
        String etag = s3.uploadPart(b -> b.bucket(ready.bucket).key(ready.objectKey).uploadId(ready.uploadId).partNumber(1),
                RequestBody.fromString(content)).eTag();
        return store.complete(VEHICLE, job.id, new ClipApi.Complete(ready.uploadId, List.of(new ClipApi.Part(1, etag))));
    }

    private void processNext(ConfigurableApplicationContext context) {
        context.getBean(ClipWorker.class).process(context.getBean(ClipStore.class).claim().orElseThrow());
    }

    private void expire(ClipJob job) {
        first.getBean(JdbcTemplate.class).update("UPDATE clip_upload_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=?", job.id);
    }

    @SuppressWarnings("unchecked")
    private void sendOutbox(ClipOutbox outbox) throws Exception {
        var record = new ProducerRecord<byte[], byte[]>(outbox.topic, outbox.recordKey.getBytes(StandardCharsets.UTF_8), outbox.payload);
        ClipJson.MAPPER.readTree(outbox.headers).properties().forEach(entry -> record.headers().add(entry.getKey(),
                entry.getValue().asString().getBytes(StandardCharsets.UTF_8)));
        first.getBean(KafkaTemplate.class).send(record).get(30, TimeUnit.SECONDS);
    }

    private List<ConsumerRecord<byte[], byte[]>> recordsFor(String id, int expected) {
        var config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "clip-test-" + UUID.randomUUID());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<byte[], byte[]>(config)) {
            consumer.assign(List.of(new TopicPartition("telemetry.records", 0)));
            consumer.seekToBeginning(consumer.assignment());
            var found = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> {
                    if (("clip-" + id).equals(header(record, "boot_id"))) found.add(record);
                });
                return found.size() >= expected;
            });
            return found;
        }
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String key) {
        var header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private ClipApi.Create request(String id, String content) {
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        return new ClipApi.Create(id, 1_800_000_000_000_000L, 1_800_000_001_000_000L, data.length,
                HexFormat.of().formatHex(ClipJson.digest().digest(data)), 16L * 1024 * 1024,
                null, List.of("lidar"), 1, Map.of("lidar", "{\"sensor\":true}"));
    }

    private Result call(MqttClient client, ConfigurableApplicationContext context, String method, String path, Object body)
            throws Exception {
        String[] parts = path.replaceFirst("^/", "").split("/");
        String operation = path.isEmpty() ? "create" : method.equals("GET") ? "status" : method.equals("DELETE") ? "cancel"
                : parts[1].replace('-', '_');
        String correlation = UUID.randomUUID().toString();
        var envelope = new HashMap<String, Object>();
        envelope.put("version", 1); envelope.put("correlation_id", correlation); envelope.put("operation", operation);
        envelope.put("job_id", path.isEmpty() ? null : parts[0]); envelope.put("body", body == null ? Map.of() : body);
        byte[] wire = json.write(envelope).getBytes(StandardCharsets.UTF_8);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            client.publish("fleet/v1/" + client.getClientId() + "/clip/request", wire, 1, false);
            byte[] response = replies.get(client).poll(1, TimeUnit.SECONDS);
            if (response == null) continue;
            var value = ClipJson.MAPPER.readTree(response);
            if (correlation.equals(value.path("correlation_id").asString()))
                return new Result(value.get("status").asInt(), value.get("data").toString());
        }
        throw new AssertionError("MQTT application reply timed out");
    }

    private record Result(int statusCode, String body) { }
    private tools.jackson.databind.JsonNode tree(Result response) { return ClipJson.MAPPER.readTree(response.body()); }

    private ConfigurableApplicationContext application() {
        return application(false);
    }

    private ConfigurableApplicationContext application(boolean workers) {
        String service = first == null || !first.isActive() ? "clip-service" : "clip-service-2";
        return new SpringApplicationBuilder(GatewayApplication.class).run(
                "--spring.datasource.url=" + postgres.getJdbcUrl(), "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(), "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--server.port=0", "--management.server.port=0", "--spring.grpc.server.port=0",
                "--fleetsentinel.clips.mqtt.enabled=" + !workers,
                "--fleetsentinel.clips.mqtt.uri=ssl://localhost:" + broker.getMappedPort(8883),
                "--fleetsentinel.clips.mqtt.client-id=" + service,
                "--fleetsentinel.clips.mqtt.pki-directory=" + mqttPki,
                "--fleetsentinel.clips.worker-enabled=" + workers, "--fleetsentinel.object-store.bucket=clip-test",
                "--fleetsentinel.object-store.endpoint=http://" + minio.getHost() + ":" + minio.getMappedPort(9000),
                "--fleetsentinel.object-store.path-style-access=true", "--fleetsentinel.object-store.credentials-mode=static",
                "--fleetsentinel.object-store.access-key=testaccess", "--fleetsentinel.object-store.secret-key=testsecret123",
                "--spring.ssl.bundle.pem.gateway.keystore.certificate=file:" + pki.resolve("server/server.crt"),
                "--spring.ssl.bundle.pem.gateway.keystore.private-key=file:" + pki.resolve("server/server.key"),
                "--spring.ssl.bundle.pem.gateway.truststore.certificate=file:" + pki.resolve("ca/ca.crt"),
                "--spring.main.banner-mode=off", "--logging.level.root=WARN");
    }

    private SSLContext mqttTls(String vehicle) throws Exception {
        var certificates = CertificateFactory.getInstance("X.509");
        var ca = certificates.generateCertificate(Files.newInputStream(mqttPki.resolve("ca.crt")));
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType()); trust.load(null);
        trust.setCertificateEntry("ca", ca);
        var tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); tm.init(trust);
        KeyManager[] keyManagers = null;
        if (vehicle != null) {
            var directory = mqttPki;
            String pem = Files.readString(directory.resolve(vehicle + ".key"))
                    .replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replaceAll("\\s", "");
            var key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
            var certificate = certificates.generateCertificate(Files.newInputStream(directory.resolve(vehicle + ".crt")));
            KeyStore keys = KeyStore.getInstance(KeyStore.getDefaultType()); keys.load(null);
            keys.setKeyEntry("vehicle", key, new char[0], new java.security.cert.Certificate[]{certificate, ca});
            var km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); km.init(keys, new char[0]);
            keyManagers = km.getKeyManagers();
        }
        var ssl = SSLContext.getInstance("TLS"); ssl.init(keyManagers, tm.getTrustManagers(), null);
        return ssl;
    }
    private MqttClient mqttClient(String vehicle) throws Exception {
        String id = vehicle == null ? "anonymous" : vehicle;
        var client = new MqttClient("ssl://localhost:" + broker.getMappedPort(8883), id, new MemoryPersistence());
        var queue = new LinkedBlockingQueue<byte[]>();
        client.setCallback(new MqttCallback() {
            public void connectionLost(Throwable cause) { }
            public void deliveryComplete(IMqttDeliveryToken token) { }
            public void messageArrived(String topic, MqttMessage message) { queue.offer(message.getPayload()); }
        });
        var options = new MqttConnectOptions();
        options.setSocketFactory(mqttTls(vehicle).getSocketFactory()); options.setConnectionTimeout(3);
        try { client.connect(options); } catch (MqttException e) { client.close(); throw e; }
        client.subscribe("fleet/v1/" + id + "/clip/response", 1);
        replies.put(client, queue);
        return client;
    }

}
