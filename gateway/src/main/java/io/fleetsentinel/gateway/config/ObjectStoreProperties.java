package io.fleetsentinel.gateway.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 중량 경로 오브젝트 스토리지 설정.
 *
 * <p>로컬은 MinIO, 운영은 S3/GCS를 가정한다. MinIO는 S3 호환이라 엔드포인트와
 * path-style만 다르다.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "fleetsentinel.object-store")
public class ObjectStoreProperties {

    /** 원시 센서 클립 버킷. */
    private String bucket = "fleet-raw";

    /** 키 스킴 버전. 스킴을 바꿔야 할 때 기존 객체를 옮기지 않고 새 접두사로 간다. */
    private String keyPrefix = "v1";

    /** MinIO 엔드포인트. 비우면 AWS 기본 엔드포인트를 쓴다. */
    private String endpoint = "http://localhost:9000";

    /**
     * MinIO는 가상 호스트 스타일(`bucket.host`)을 기본 지원하지 않는다.
     * 로컬에서는 반드시 true여야 한다.
     */
    private boolean pathStyleAccess = true;

    private String region = "us-east-1";
    private String accessKey = "admin";
    private String secretKey = "password";

    /**
     * presigned URL 만료.
     *
     * <p>최악 클립 업로드가 42초다.
     * 20배 여유를 두되, 길수록 유출된 URL의 창이 커진다.
     */
    private Duration presignTtl = Duration.ofMinutes(15);

    /**
     * 허용하는 최대 파트 수. 차량이 보낸 `part_size_bytes`가 너무 작으면 URL을
     * 수천 개 서명하게 되므로 상한을 둔다.
     */
    private int maxParts = 1000;

    /** S3 규약상 마지막 파트를 뺀 모든 파트는 5 MiB 이상이어야 한다. */
    private long minPartSizeBytes = 5L * 1024 * 1024;
}
