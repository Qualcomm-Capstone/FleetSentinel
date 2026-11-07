package io.fleetsentinel.gateway.clip;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "clip_upload_jobs", uniqueConstraints = @UniqueConstraint(columnNames = {"vehicle_id", "request_id"}))
@Getter
@NoArgsConstructor
public class ClipJob {
    @Id @Column(length = 26) String id;
    @Version long version;
    @Column(name = "vehicle_id", nullable = false, length = 64) String vehicleId;
    @Column(name = "request_id", nullable = false, length = 160) String requestId;
    @Column(nullable = false, length = 64) String fingerprint;
    @Column(nullable = false, columnDefinition = "text") String metadata;
    @Column(nullable = false) String bucket;
    @Column(name = "object_key", nullable = false, columnDefinition = "text") String objectKey;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 24) ClipState state;
    @Column(name = "upload_id", columnDefinition = "text") String uploadId;
    @Column(name = "complete_parts", columnDefinition = "text") String completeParts;
    @Column(name = "actual_sha256", length = 64) String actualSha256;
    @Column(name = "actual_size_bytes") Long actualSizeBytes;
    @Column(name = "lease_token", length = 36) String leaseToken;
    @Column(name = "lease_until") Instant leaseUntil;
    @Column(name = "next_attempt", nullable = false) Instant nextAttempt;
    @Column(name = "created_at", nullable = false) Instant createdAt;
    @Column(name = "updated_at", nullable = false) Instant updatedAt;
    @Column(nullable = false) int attempts;
    @Column(columnDefinition = "text") String error;

    ClipApi.View view() {
        var request = new ClipJson().read(metadata);
        return new ClipApi.View(id, state, uploadId, "s3://" + bucket + "/" + objectKey,
                request.sizeBytes(), request.sha256(), request.partSizeBytes(), error);
    }
}
