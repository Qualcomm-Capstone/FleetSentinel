package io.fleetsentinel.gateway.clip;

import io.fleetsentinel.gateway.config.GatewayProperties;
import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import io.fleetsentinel.gateway.storage.SegmentKey;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/** Short DB-only transactions. No object-store or Kafka operation is allowed in this class. */
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "fleetsentinel.clips.enabled", havingValue = "true", matchIfMissing = true)
public class ClipStore {
    private final EntityManager em;
    private final TransactionTemplate tx;
    private final ObjectStoreProperties objects;
    private final GatewayProperties gateway;
    private final ClipProperties properties;
    private final ClipReferenceEncoder encoder;
    private final ClipJson json = new ClipJson();

    public ClipJob create(String vehicle, ClipApi.Create request) {
        request.validate(objects);
        String fingerprint = json.fingerprint(request);
        return tx.execute(status -> {
            String id = ClipUlid.next();
            Instant now = now();
            em.createNativeQuery("""
                    INSERT INTO clip_upload_jobs
                      (id,version,vehicle_id,request_id,fingerprint,metadata,bucket,object_key,
                       state,next_attempt,created_at,updated_at,attempts)
                    VALUES (?1,0,?2,?3,?4,?5,?6,?7,'CREATING',?8,?8,?8,0)
                    ON CONFLICT (vehicle_id,request_id) DO NOTHING
                    """)
                    .setParameter(1, id).setParameter(2, vehicle).setParameter(3, request.requestId())
                    .setParameter(4, fingerprint).setParameter(5, json.write(request))
                    .setParameter(6, objects.getBucket())
                    .setParameter(7, SegmentKey.of(objects.getKeyPrefix(), vehicle, id, request.tStartUs()))
                    .setParameter(8, now).executeUpdate();
            ClipJob job = em.createQuery("select j from ClipJob j where j.vehicleId=:v and j.requestId=:r", ClipJob.class)
                    .setParameter("v", vehicle).setParameter("r", request.requestId()).getSingleResult();
            if (!job.fingerprint.equals(fingerprint)) throw conflict("request_id metadata differs from the accepted request");
            return job;
        });
    }

    public ClipJob get(String vehicle, String id) {
        return tx.execute(status -> owned(vehicle, id, false));
    }

    public ClipJob complete(String vehicle, String id, ClipApi.Complete request) {
        return tx.execute(status -> {
            ClipJob job = owned(vehicle, id, true);
            ClipApi.require(request.uploadId() != null && !request.uploadId().isBlank(), "upload_id is required");
            if (!request.uploadId().equals(job.uploadId)) throw conflict("upload_id changed; refresh job and re-upload all parts");
            ClipApi.validateParts(request.parts(), json.read(job.metadata).partCount());
            String parts = json.write(new ClipApi.Complete(request.uploadId(), request.parts().stream()
                    .sorted(Comparator.comparingInt(ClipApi.Part::partNumber)).toList()));
            if (job.completeParts != null) {
                if (!job.completeParts.equals(parts)) throw conflict("complete parts differ from the accepted manifest");
                return job;
            }
            if (job.state != ClipState.UPLOADING) throw conflict("upload is not ready to complete");
            job.completeParts = parts;
            job.state = ClipState.VERIFYING;
            job.nextAttempt = now();
            job.updatedAt = now();
            // A running UPLOADING reconciliation holds its lease until it finishes/ages out.
            // Its fenced result cannot overwrite VERIFYING.
            return job;
        });
    }

    public ClipJob cancel(String vehicle, String id) {
        return tx.execute(status -> {
            ClipJob job = owned(vehicle, id, true);
            if (job.state == ClipState.ABORTING || job.state == ClipState.ABORTED) return job;
            if (job.state != ClipState.CREATING && job.state != ClipState.UPLOADING)
                throw conflict("verification has started; cancellation is no longer allowed");
            job.state = ClipState.ABORTING;
            job.nextAttempt = now();
            job.updatedAt = now();
            // Do not erase the old lease: an outstanding S3 create must finish before cleanup.
            return job;
        });
    }

    public Optional<ClipJob> claim() {
        return tx.execute(status -> {
            @SuppressWarnings("unchecked")
            List<ClipJob> jobs = em.createNativeQuery("""
                    SELECT * FROM clip_upload_jobs
                    WHERE state IN ('CREATING','UPLOADING','VERIFYING','STORED','ABORTING')
                      AND next_attempt <= clock_timestamp()
                      AND (lease_until IS NULL OR lease_until < clock_timestamp())
                    ORDER BY next_attempt, id LIMIT 1 FOR UPDATE SKIP LOCKED
                    """, ClipJob.class).getResultList();
            if (jobs.isEmpty()) return Optional.empty();
            ClipJob job = jobs.getFirst();
            job.leaseToken = UUID.randomUUID().toString();
            job.leaseUntil = now().plusSeconds(properties.getLeaseSeconds());
            job.attempts++;
            job.updatedAt = now();
            return Optional.of(job);
        });
    }

    public boolean renew(ClipJob claim) {
        return fenced(claim, job -> job.leaseUntil = now().plusSeconds(properties.getLeaseSeconds()), false);
    }

    public boolean uploading(ClipJob claim, String uploadId) {
        return fenced(claim, job -> {
            job.uploadId = uploadId;
            job.state = ClipState.UPLOADING;
            job.error = null;
            job.nextAttempt = now().plusSeconds(60);
        }, true);
    }

    public boolean reconciled(ClipJob claim) {
        return fenced(claim, job -> job.nextAttempt = now().plusSeconds(60), true);
    }

    /** The immutable clip identity survives object-store expiry; the upload ID is the transfer epoch. */
    public boolean restarted(ClipJob claim, String uploadId) {
        if (claim.state != ClipState.UPLOADING && claim.state != ClipState.VERIFYING)
            throw new IllegalArgumentException("only an unverified transfer can restart");
        return fenced(claim, job -> {
            job.uploadId = uploadId;
            job.completeParts = null;
            job.state = ClipState.UPLOADING;
            job.error = "multipart_expired_restarted";
            job.attempts = 0;
            job.nextAttempt = now().plusSeconds(60);
        }, true);
    }

    public boolean verified(ClipJob claim, String digest, long size, String failure) {
        return fenced(claim, job -> {
            job.actualSha256 = digest;
            job.actualSizeBytes = size;
            job.error = failure;
            if (failure != null) {
                job.state = ClipState.FAILED;
                return;
            }
            ClipOutbox outbox = new ClipOutbox();
            outbox.jobId = job.id;
            outbox.topic = gateway.getTopics().getRecords();
            outbox.recordKey = job.vehicleId;
            outbox.headers = json.write(Map.of("vehicle_id", job.vehicleId, "boot_id", "clip-" + job.id,
                    "seq", "0", "kind", "RECORD_KIND_SEGMENT_REF", "producer_origin", "clip-upload"));
            outbox.payload = encoder.encode(job, json.read(job.metadata), digest);
            outbox.createdAt = now();
            em.persist(outbox);
            job.state = ClipState.STORED;
            job.nextAttempt = now();
        }, true);
    }

    public ClipOutbox outbox(String id) {
        return tx.execute(status -> em.find(ClipOutbox.class, id));
    }

    public boolean published(ClipJob claim) {
        return fenced(claim, job -> {
            ClipOutbox outbox = em.find(ClipOutbox.class, job.id);
            if (outbox == null) throw new IllegalStateException("missing durable clip outbox");
            outbox.publishedAt = now();
            job.state = ClipState.COMPLETED;
            job.error = null;
        }, true);
    }

    public boolean aborted(ClipJob claim) {
        return fenced(claim, job -> { job.state = ClipState.ABORTED; job.error = null; }, true);
    }

    public boolean retry(ClipJob claim, String reason, boolean terminal) {
        return fenced(claim, job -> {
            job.error = reason;
            if (terminal) job.state = ClipState.FAILED;
            else job.nextAttempt = now().plusSeconds(Math.min(300, 2L << Math.min(job.attempts, 7)));
        }, true);
    }

    private boolean fenced(ClipJob claim, Consumer<ClipJob> change, boolean release) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            ClipJob job = em.find(ClipJob.class, claim.id, LockModeType.PESSIMISTIC_WRITE);
            if (job == null || !claim.leaseToken.equals(job.leaseToken)
                    || job.state != claim.state || job.leaseUntil == null || !job.leaseUntil.isAfter(now())) return false;
            change.accept(job);
            job.updatedAt = now();
            if (release) { job.leaseUntil = null; job.leaseToken = null; }
            return true;
        }));
    }

    private ClipJob owned(String vehicle, String id, boolean lock) {
        ClipJob job = lock ? em.find(ClipJob.class, id, LockModeType.PESSIMISTIC_WRITE) : em.find(ClipJob.class, id);
        if (job == null || !job.vehicleId.equals(vehicle))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "clip upload not found");
        return job;
    }

    private Instant now() {
        return (Instant) em.createNativeQuery("select clock_timestamp()", Instant.class).getSingleResult();
    }

    private static ResponseStatusException conflict(String reason) {
        return new ResponseStatusException(HttpStatus.CONFLICT, reason);
    }
}
