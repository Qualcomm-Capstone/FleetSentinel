package io.fleetsentinel.gateway.clip;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Two workers per instance, no in-memory backlog; the database is the durable queue. */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "fleetsentinel.clips.enabled", havingValue = "true", matchIfMissing = true)
public class ClipWorker {
    private final ClipStore store;
    private final ClipObjectStore objects;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final ClipProperties properties;
    private final Semaphore slots = new Semaphore(2);
    private final ExecutorService workers = Executors.newFixedThreadPool(2,
            Thread.ofPlatform().name("clip-worker-", 0).factory());
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2,
            Thread.ofPlatform().name("clip-control-", 0).factory());

    @PostConstruct
    void start() {
        if (properties.isWorkerEnabled()) scheduler.scheduleWithFixedDelay(this::poll, 0, 500, TimeUnit.MILLISECONDS);
    }

    private void poll() {
        try {
            while (slots.tryAcquire()) {
                var claim = store.claim();
                if (claim.isEmpty()) { slots.release(); return; }
                try { workers.submit(() -> { try { process(claim.get()); } finally { slots.release(); } }); }
                catch (RejectedExecutionException e) { slots.release(); return; }
            }
        } catch (RuntimeException e) {
            // A failed claim must not permanently consume capacity or stop the scheduled task.
            if (slots.availablePermits() < 2 && !workers.isShutdown()) slots.release();
            log.warn("Clip queue polling failed: {}", e.getClass().getSimpleName());
        }
    }

    /** Exposed package-wide to exercise crash boundaries with real external services. */
    void process(ClipJob claim) {
        var alive = new AtomicBoolean(true);
        var heartbeat = scheduler.scheduleAtFixedRate(() -> {
            try { if (!store.renew(claim)) alive.set(false); }
            catch (RuntimeException e) { alive.set(false); }
        }, 10, 10, TimeUnit.SECONDS);
        try {
            switch (claim.state) {
                case CREATING -> store.uploading(claim, objects.createOrRecover(claim));
                case UPLOADING -> {
                    String uploadId = objects.reconcile(claim);
                    if (uploadId.equals(claim.uploadId)) store.reconciled(claim);
                    else store.restarted(claim, uploadId);
                }
                case ABORTING -> { objects.abort(claim); store.aborted(claim); }
                case VERIFYING -> {
                    try {
                        var result = objects.verify(claim, alive::get);
                        store.verified(claim, result.digest(), result.size(), result.failure());
                    } catch (ClipObjectStore.MissingMultipartUpload expired) {
                        store.restarted(claim, objects.createOrRecover(claim));
                    }
                }
                case STORED -> {
                    ClipOutbox outbox = store.outbox(claim.id);
                    if (outbox == null) throw new IllegalStateException("missing clip outbox");
                    var record = new ProducerRecord<byte[], byte[]>(outbox.topic,
                            outbox.recordKey.getBytes(StandardCharsets.UTF_8), outbox.payload);
                    ClipJson.MAPPER.readTree(outbox.headers).properties().forEach(entry ->
                            record.headers().add(entry.getKey(), entry.getValue().asString().getBytes(StandardCharsets.UTF_8)));
                    // Only a broker ACK allows the DB transaction to expose COMPLETED.
                    kafka.send(record).get(125, TimeUnit.SECONDS);
                    store.published(claim);
                }
                default -> throw new IllegalStateException("unexpected leased clip state");
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            String reason = "retryable_" + e.getClass().getSimpleName();
            boolean terminal = false;
            if (e instanceof S3Exception s3) {
                reason = "object_store_" + ClipObjectStore.code(s3);
                terminal = java.util.Set.of("InvalidPart", "InvalidPartOrder", "EntityTooSmall").contains(ClipObjectStore.code(s3));
            }
            log.warn("Clip work deferred: id={} state={} reason={}", claim.id, claim.state, reason);
            try { store.retry(claim, reason, terminal); }
            catch (RuntimeException dbFailure) { log.warn("Clip retry persistence failed: id={}", claim.id); }
        } finally { heartbeat.cancel(false); }
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
        workers.shutdownNow();
        try { workers.awaitTermination(10, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
