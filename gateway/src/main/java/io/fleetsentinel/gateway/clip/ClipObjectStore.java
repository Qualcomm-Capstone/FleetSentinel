package io.fleetsentinel.gateway.clip;

import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "fleetsentinel.clips.enabled", havingValue = "true", matchIfMissing = true)
public class ClipObjectStore {
    private final S3Client s3;
    private final S3Presigner presigner;
    private final ObjectStoreProperties properties;
    private final ClipJson json = new ClipJson();

    /** Recover a create whose response or DB commit was lost; compare full keys, never prefix alone. */
    String createOrRecover(ClipJob job) {
        List<MultipartUpload> pending = uploads(job);
        String chosen;
        if (pending.isEmpty()) {
            chosen = s3.createMultipartUpload(b -> b.bucket(job.bucket).key(job.objectKey)
                    .contentType("application/octet-stream")
                    .metadata(Map.of("sha256", json.read(job.metadata).sha256(),
                            "vehicle-id", job.vehicleId, "segment-id", job.id))).uploadId();
        } else {
            chosen = pending.stream().min(Comparator.comparing(MultipartUpload::initiated)
                    .thenComparing(MultipartUpload::uploadId)).orElseThrow().uploadId();
        }
        abortOthers(job, pending, chosen);
        return chosen;
    }

    String reconcile(ClipJob job) {
        var active = uploads(job);
        if (active.stream().noneMatch(upload -> job.uploadId.equals(upload.uploadId())) && !exists(job)) {
            return createOrRecover(job);
        }
        abortOthers(job, active, job.uploadId);
        return job.uploadId;
    }

    void abort(ClipJob job) {
        // The saved ID is included even if list results temporarily omit it.
        if (job.uploadId != null) abortOne(job, job.uploadId);
        abortOthers(job, uploads(job), null);
    }

    ClipApi.Urls urls(ClipJob job, List<Integer> numbers) {
        if (job.state != ClipState.UPLOADING)
            throw new ResponseStatusException(HttpStatus.CONFLICT, "upload is not accepting parts");
        int count = json.read(job.metadata).partCount();
        ClipApi.require(numbers != null && !numbers.isEmpty() && numbers.size() <= count
                        && new java.util.HashSet<>(numbers).size() == numbers.size()
                        && numbers.stream().allMatch(n -> n != null && n >= 1 && n <= count),
                "part_numbers must be unique and within the clip's part count");
        Instant expires = Instant.now().plus(properties.getPresignTtl());
        var urls = numbers.stream().map(number -> new ClipApi.PartUrl(number,
                presigner.presignUploadPart(b -> b.signatureDuration(properties.getPresignTtl())
                        .uploadPartRequest(p -> p.bucket(job.bucket).key(job.objectKey)
                                .uploadId(job.uploadId).partNumber(number))).url().toString())).toList();
        return new ClipApi.Urls(job.uploadId, urls, Math.addExact(Math.multiplyExact(expires.getEpochSecond(), 1_000_000L),
                expires.getNano() / 1_000L));
    }

    Verification verify(ClipJob job, BooleanSupplier leaseAlive) {
        if (!exists(job)) {
            var parts = json.parts(job.completeParts).parts().stream()
                    .map(p -> CompletedPart.builder().partNumber(p.partNumber()).eTag(p.etag()).build()).toList();
            try {
                s3.completeMultipartUpload(b -> b.bucket(job.bucket).key(job.objectKey).uploadId(job.uploadId)
                        .multipartUpload(m -> m.parts(parts)));
            } catch (S3Exception e) {
                // An ambiguous Complete may have committed; verification of the deterministic key is authoritative.
                if (!"NoSuchUpload".equals(code(e))) throw e;
                if (!exists(job)) throw new MissingMultipartUpload();
            }
        }
        var request = json.read(job.metadata);
        var digest = ClipJson.digest();
        long bytes = 0;
        try (var stream = s3.getObject(b -> b.bucket(job.bucket).key(job.objectKey))) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = stream.read(buffer)) != -1) {
                if (!leaseAlive.getAsBoolean()) {
                    stream.abort();
                    throw new IllegalStateException("clip worker lease lost");
                }
                digest.update(buffer, 0, read);
                bytes += read;
            }
            String actual = HexFormat.of().formatHex(digest.digest());
            var metadata = stream.response().metadata();
            String failure = null;
            if (bytes != request.sizeBytes()) failure = "size_mismatch";
            else if (!request.sha256().equals(actual)) failure = "sha256_mismatch";
            else if (!job.vehicleId.equals(metadata.get("vehicle-id")) || !job.id.equals(metadata.get("segment-id"))
                    || !request.sha256().equals(metadata.get("sha256"))) failure = "object_identity_mismatch";
            return new Verification(actual, bytes, failure);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private boolean exists(ClipJob job) {
        try { s3.headObject(b -> b.bucket(job.bucket).key(job.objectKey)); return true; }
        catch (S3Exception e) { if (e.statusCode() == 404) return false; throw e; }
    }

    private List<MultipartUpload> uploads(ClipJob job) {
        List<MultipartUpload> uploads = new ArrayList<>();
        for (var page : s3.listMultipartUploadsPaginator(b -> b.bucket(job.bucket).prefix(job.objectKey))) {
            for (var upload : page.uploads()) if (job.objectKey.equals(upload.key())) uploads.add(upload);
        }
        return uploads;
    }

    private void abortOthers(ClipJob job, List<MultipartUpload> uploads, String keep) {
        for (var upload : uploads) if (!upload.uploadId().equals(keep)) abortOne(job, upload.uploadId());
    }

    private void abortOne(ClipJob job, String id) {
        try { s3.abortMultipartUpload(b -> b.bucket(job.bucket).key(job.objectKey).uploadId(id)); }
        catch (S3Exception e) { if (!"NoSuchUpload".equals(code(e))) throw e; }
    }

    static String code(S3Exception error) {
        return error.awsErrorDetails() == null ? "" : error.awsErrorDetails().errorCode();
    }

    record Verification(String digest, long size, String failure) { }
    static final class MissingMultipartUpload extends RuntimeException { }
}
