package io.fleetsentinel.gateway.clip;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "clip_upload_outbox")
@Getter
@NoArgsConstructor
public class ClipOutbox {
    @Id @Column(name = "job_id", length = 26) String jobId;
    @Column(nullable = false, updatable = false) String topic;
    @Column(name = "record_key", nullable = false, updatable = false) String recordKey;
    @Column(nullable = false, updatable = false, columnDefinition = "text") String headers;
    @Column(nullable = false, updatable = false, columnDefinition = "bytea") byte[] payload;
    @Column(name = "created_at", nullable = false, updatable = false) Instant createdAt;
    @Column(name = "published_at") Instant publishedAt;
}
