package io.fleetsentinel.gateway.grpc;

import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import io.fleetsentinel.gateway.identity.PeerIdentityInterceptor;
import io.fleetsentinel.gateway.identity.VehicleIdentity;
import io.fleetsentinel.gateway.proto.AbortUploadRequest;
import io.fleetsentinel.gateway.proto.AbortUploadResponse;
import io.fleetsentinel.gateway.proto.BeginUploadRequest;
import io.fleetsentinel.gateway.proto.BeginUploadResponse;
import io.fleetsentinel.gateway.proto.CompleteUploadRequest;
import io.fleetsentinel.gateway.proto.CompleteUploadResponse;
import io.fleetsentinel.gateway.proto.PartUrl;
import io.fleetsentinel.gateway.storage.SegmentKey;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.grpc.server.service.GrpcService;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;

/**
 * 중량 경로 제어 평면. <b>데이터는 지나가지 않는다.</b>
 *
 * <p>게이트웨이가 하는 일은 셋뿐이다 — 신원 확정, 그 신원의 경로로만 유효한 presigned URL
 * 발급, 완료 시 무결성 확인. 298~341 MiB의 MCAP은 차량 → 오브젝트 스토리지로 직행한다.
 *
 * <p><b>stateless를 유지한다.</b> 어느 파트까지 올렸는지는 차량과 스토리지가 알고 있고,
 * 게이트웨이는 물어보면 URL을 새로 서명해줄 뿐이다. 경량 경로에서 재개 지점을 차량이 정한
 * 것과 같은 원칙이다(SDD S-9).
 */
@GrpcService
@Slf4j
public class SegmentUploadService
        extends io.fleetsentinel.gateway.proto.SegmentUploadGrpc.SegmentUploadImplBase {

    private final S3Client s3;
    private final S3Presigner presigner;
    private final ObjectStoreProperties props;
    private final Counter begun;
    private final Counter completed;
    private final Counter checksumMismatch;
    private final Counter aborted;

    public SegmentUploadService(S3Client s3, S3Presigner presigner,
                                ObjectStoreProperties props, MeterRegistry meters) {
        this.s3 = s3;
        this.presigner = presigner;
        this.props = props;
        this.begun = meters.counter("fleetsentinel.gateway.segments.begun");
        this.completed = meters.counter("fleetsentinel.gateway.segments.completed");
        this.checksumMismatch = meters.counter("fleetsentinel.gateway.segments.checksum_mismatch");
        this.aborted = meters.counter("fleetsentinel.gateway.segments.aborted");
    }

    @Override
    public void begin(BeginUploadRequest req, StreamObserver<BeginUploadResponse> out) {
        VehicleIdentity identity = PeerIdentityInterceptor.IDENTITY.get();
        if (identity == null) {
            out.onError(Status.INTERNAL
                    .withDescription("identity interceptor not applied").asRuntimeException());
            return;
        }

        try {
            validate(req);
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }

        // segment_id는 **게이트웨이가** 발급한다. 차량이 정하면 남의 클립 id를 주장해
        // 카탈로그를 오염시킬 수 있다.
        String segmentId = Ulid.next();
        String key;
        try {
            key = SegmentKey.of(props.getKeyPrefix(), identity.vehicleId(),
                    segmentId, req.getTStartUs());
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }

        int partCount = (int) Math.max(1,
                (req.getSizeBytes() + req.getPartSizeBytes() - 1) / req.getPartSizeBytes());
        if (partCount > props.getMaxParts()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(
                    "파트 수 " + partCount + "가 상한 " + props.getMaxParts() + "을 넘는다 — "
                            + "part_size_bytes를 키운다").asRuntimeException());
            return;
        }

        try {
            var created = s3.createMultipartUpload(b -> b
                    .bucket(props.getBucket())
                    .key(key)
                    // sha256을 객체 메타데이터로 남긴다. multipart ETag는 MD5-of-MD5라
                    // 콘텐츠 해시가 아니다(§4.3).
                    .metadata(java.util.Map.of(
                            "sha256", req.getSha256(),
                            "vehicle-id", identity.vehicleId(),
                            "segment-id", segmentId)));

            Instant expiry = Instant.now().plus(props.getPresignTtl());
            List<Integer> numbers = new ArrayList<>(partCount);
            for (int n = 1; n <= partCount; n++) {
                numbers.add(n);
            }
            List<PartUrl> urls = presign(key, created.uploadId(), numbers);

            begun.increment();
            log.info("업로드 시작: vehicle={} segment={} parts={} key={}",
                    identity.vehicleId(), segmentId, partCount, key);

            out.onNext(BeginUploadResponse.newBuilder()
                    .setSegmentId(segmentId)
                    .setUploadId(created.uploadId())
                    .setBlobUri("s3://" + props.getBucket() + "/" + key)
                    .addAllPartUrls(urls)
                    .setExpiresAtUs(expiry.getEpochSecond() * 1_000_000L
                            + expiry.getNano() / 1_000L)
                    .build());
            out.onCompleted();
        } catch (RuntimeException e) {
            log.error("업로드 시작 실패: vehicle={} key={}", identity.vehicleId(), key, e);
            out.onError(Status.UNAVAILABLE
                    .withDescription("object store: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void complete(CompleteUploadRequest req, StreamObserver<CompleteUploadResponse> out) {
        VehicleIdentity identity = PeerIdentityInterceptor.IDENTITY.get();
        if (identity == null) {
            out.onError(Status.INTERNAL
                    .withDescription("identity interceptor not applied").asRuntimeException());
            return;
        }
        if (req.getPartsCount() == 0) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("parts가 비어 있다").asRuntimeException());
            return;
        }

        String key;
        try {
            key = keyFor(identity.vehicleId(), req.getSegmentId(), req.getTStartUs());
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }

        List<CompletedPart> parts = req.getPartsList().stream()
                .sorted(Comparator.comparingInt(p -> p.getPartNumber()))
                .map(p -> CompletedPart.builder()
                        .partNumber(p.getPartNumber())
                        .eTag(p.getEtag())
                        .build())
                .toList();

        try {
            try {
                s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(props.getBucket())
                        .key(key)
                        .uploadId(req.getUploadId())
                        .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                        .build());
            } catch (S3Exception e) {
                if (!isNoSuchUpload(e)) {
                    throw e;
                }
                // Complete는 서버에서 성공했지만 응답을 받기 전에 차량이 죽을 수 있다.
                // multipart는 이미 사라졌으므로 재호출은 NoSuchUpload다. 최종 키의 객체를
                // 다시 검증해 같은 성공 응답을 돌려 Complete를 멱등하게 만든다.
                log.info("완료 재확인: vehicle={} segment={} upload={} — 최종 객체 검증",
                        identity.vehicleId(), req.getSegmentId(), req.getUploadId());
            }

            var head = s3.headObject(HeadObjectRequest.builder()
                    .bucket(props.getBucket()).key(key).build());
            String expected = head.metadata().get("sha256");
            String actual = sha256Of(key);

            boolean verified = expected != null
                    && expected.equalsIgnoreCase(actual)
                    && identity.vehicleId().equals(head.metadata().get("vehicle-id"))
                    && req.getSegmentId().equals(head.metadata().get("segment-id"));
            if (!verified) {
                // 검증 실패한 객체를 남겨두면 카탈로그에 없는 쓰레기가 된다.
                checksumMismatch.increment();
                s3.deleteObject(b -> b.bucket(props.getBucket()).key(key));
                log.warn("sha256 불일치 — 객체 삭제: vehicle={} segment={} 기대={} 실제={}",
                        identity.vehicleId(), req.getSegmentId(), expected, actual);
                out.onError(Status.DATA_LOSS.withDescription(
                        "sha256 mismatch: expected=" + expected + " actual=" + actual)
                        .asRuntimeException());
                return;
            }

            completed.increment();
            log.info("업로드 완료: vehicle={} segment={} size={} key={}",
                    identity.vehicleId(), req.getSegmentId(), head.contentLength(), key);

            out.onNext(CompleteUploadResponse.newBuilder()
                    .setBlobUri("s3://" + props.getBucket() + "/" + key)
                    .setSizeBytes(head.contentLength())
                    .setVerified(true)
                    .build());
            out.onCompleted();
        } catch (RuntimeException e) {
            log.error("업로드 완료 실패: vehicle={} segment={}",
                    identity.vehicleId(), req.getSegmentId(), e);
            out.onError(Status.UNAVAILABLE
                    .withDescription("object store: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void refresh(io.fleetsentinel.gateway.proto.RefreshUrlsRequest req,
                        StreamObserver<BeginUploadResponse> out) {
        VehicleIdentity identity = PeerIdentityInterceptor.IDENTITY.get();
        if (identity == null) {
            out.onError(Status.INTERNAL
                    .withDescription("identity interceptor not applied").asRuntimeException());
            return;
        }
        List<Integer> numbers = new ArrayList<>(req.getPartNumbersList());
        if (numbers.isEmpty()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(
                    "part_numbers 가 비었다 — 차량이 남은 파트 번호를 보내야 한다")
                    .asRuntimeException());
            return;
        }
        String key;
        try {
            key = keyFor(identity.vehicleId(), req.getSegmentId(), req.getTStartUs());
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        try {
            Instant expiry = Instant.now().plus(props.getPresignTtl());
            out.onNext(BeginUploadResponse.newBuilder()
                    .setSegmentId(req.getSegmentId())
                    .setUploadId(req.getUploadId())
                    .setBlobUri("s3://" + props.getBucket() + "/" + key)
                    .addAllPartUrls(presign(key, req.getUploadId(), numbers))
                    .setExpiresAtUs(expiry.getEpochSecond() * 1_000_000L
                            + expiry.getNano() / 1_000L)
                    .build());
            out.onCompleted();
        } catch (RuntimeException e) {
            out.onError(Status.UNAVAILABLE
                    .withDescription("object store: " + e.getMessage()).asRuntimeException());
        }
    }

    /** 파트 번호가 서명에 들어가므로 파트마다 개별로 서명한다. */
    private List<PartUrl> presign(String key, String uploadId, List<Integer> partNumbers) {
        List<PartUrl> urls = new ArrayList<>(partNumbers.size());
        for (int n : partNumbers) {
            final int partNumber = n;
            var presigned = presigner.presignUploadPart(UploadPartPresignRequest.builder()
                    .signatureDuration(props.getPresignTtl())
                    .uploadPartRequest(u -> u
                            .bucket(props.getBucket())
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber))
                    .build());
            urls.add(PartUrl.newBuilder()
                    .setPartNumber(n)
                    .setUrl(presigned.url().toString())
                    .build());
        }
        return urls;
    }

    @Override
    public void abort(AbortUploadRequest req, StreamObserver<AbortUploadResponse> out) {
        VehicleIdentity identity = PeerIdentityInterceptor.IDENTITY.get();
        if (identity == null) {
            out.onError(Status.INTERNAL
                    .withDescription("identity interceptor not applied").asRuntimeException());
            return;
        }
        String key;
        try {
            key = keyFor(identity.vehicleId(), req.getSegmentId(), req.getTStartUs());
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        try {
            s3.abortMultipartUpload(b -> b
                    .bucket(props.getBucket()).key(key).uploadId(req.getUploadId()));
            aborted.increment();
            out.onNext(AbortUploadResponse.newBuilder().setAborted(true).build());
            out.onCompleted();
        } catch (RuntimeException e) {
            out.onError(Status.UNAVAILABLE
                    .withDescription("object store: " + e.getMessage()).asRuntimeException());
        }
    }

    /**
     * 키를 <b>재구성</b>한다. 차량이 보낸 키를 쓰지 않는다.
     *
     * <p><b>목록 스캔을 쓰지 않는 이유</b> — 초안은 {@code listMultipartUploads}로 이 차량의
     * 접두사를 훑어 `segment_id`에 맞는 키를 찾았다. 두 가지가 나빴다: 호출마다
     * O(진행 중 업로드)이고, MinIO에서 접두사 필터가 기대대로 동작하지 않았다.
     *
     * <p>키는 {@code (vehicle_id, segment_id, t_start)}의 순수 함수다(SegmentKey).
     * 그 셋 중 <b>vehicle_id는 인증서에서 오므로</b> 나머지를 클라이언트가 보내도 남의
     * 접두사로 넘어갈 수 없다 — 신원 경계는 그대로 유지된다.
     */
    private String keyFor(String vehicleId, String segmentId, long tStartUs) {
        return SegmentKey.of(props.getKeyPrefix(), vehicleId, segmentId, tStartUs);
    }

    private static boolean isNoSuchUpload(S3Exception e) {
        return e.awsErrorDetails() != null
                && "NoSuchUpload".equals(e.awsErrorDetails().errorCode());
    }

    /** 객체를 다시 읽어 sha256을 계산한다. 스트리밍이라 메모리에 전체를 올리지 않는다. */
    private String sha256Of(String key) {
        try (var in = s3.getObject(GetObjectRequest.builder()
                .bucket(props.getBucket()).key(key).build())) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) > 0) {
                md.update(buf, 0, n);
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        } catch (java.io.IOException e) {
            throw new RuntimeException("객체를 다시 읽지 못했다: " + key, e);
        }
    }

    private void validate(BeginUploadRequest req) {
        if (req.getSizeBytes() <= 0) {
            throw new IllegalArgumentException("size_bytes > 0 이어야 한다");
        }
        if (req.getSha256().length() != 64) {
            throw new IllegalArgumentException("sha256은 hex 64자여야 한다");
        }
        if (req.getTStartUs() <= 0 || req.getTEndUs() < req.getTStartUs()) {
            throw new IllegalArgumentException("t_start_us / t_end_us 범위가 잘못됐다");
        }
        long part = req.getPartSizeBytes();
        // 파트가 하나뿐이면 S3의 5 MiB 하한이 적용되지 않는다(마지막 파트는 예외).
        if (part < props.getMinPartSizeBytes() && req.getSizeBytes() > part) {
            throw new IllegalArgumentException(
                    "part_size_bytes는 " + props.getMinPartSizeBytes() + " 이상이어야 한다");
        }
    }

    /** ULID 생성. 시간 정렬 가능해서 파일명만으로 대략의 생성 순서를 안다(§3). */
    static final class Ulid {
        private static final char[] B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
        private static final java.security.SecureRandom RNG = new java.security.SecureRandom();

        static String next() {
            long ts = System.currentTimeMillis();
            byte[] rand = new byte[10];
            RNG.nextBytes(rand);
            StringBuilder sb = new StringBuilder(26);
            for (int i = 9; i >= 0; i--) {
                sb.append(B32[(int) ((ts >>> (i * 5)) & 0x1F)]);
            }
            int acc = 0, bits = 0;
            for (byte b : rand) {
                acc = (acc << 8) | (b & 0xFF);
                bits += 8;
                while (bits >= 5) {
                    bits -= 5;
                    sb.append(B32[(acc >>> bits) & 0x1F]);
                }
            }
            return sb.substring(0, 26);
        }
    }
}
