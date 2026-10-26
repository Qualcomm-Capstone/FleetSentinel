package io.fleetsentinel.gateway.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import io.fleetsentinel.gateway.identity.PeerIdentityInterceptor;
import io.fleetsentinel.gateway.identity.VehicleIdentity;
import io.fleetsentinel.gateway.proto.CompleteUploadRequest;
import io.fleetsentinel.gateway.proto.CompleteUploadResponse;
import io.fleetsentinel.gateway.proto.PartEtag;
import io.fleetsentinel.gateway.proto.BeginUploadRequest;
import io.fleetsentinel.gateway.proto.BeginUploadResponse;
import io.fleetsentinel.gateway.proto.RefreshUrlsRequest;
import io.fleetsentinel.gateway.proto.AbortUploadRequest;
import io.fleetsentinel.gateway.proto.AbortUploadResponse;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.List;
import java.util.stream.Stream;
import java.util.function.Consumer;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedUploadPartRequest;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

class SegmentUploadServiceTest {

    private final S3Client s3 = mock(S3Client.class);
    private final S3Presigner presigner = mock(S3Presigner.class);
    private final SegmentUploadService service = new SegmentUploadService(
            s3, presigner, new ObjectStoreProperties(), new SimpleMeterRegistry());

    @ParameterizedTest(name = "complete verifies final object, previous response lost={0}")
    @ValueSource(booleans = {false, true})
    void verifiesCompletedObjectIncludingRetryAfterReplyLoss(boolean responseLost) throws Exception {
        // Given
        byte[] body = "verified-object".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String digest = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(body));

        if (responseLost) {
            when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                    .thenThrow(s3Failure(404, "NoSuchUpload"));
        }
        when(s3.headObject(any(HeadObjectRequest.class))).thenReturn(
                HeadObjectResponse.builder()
                        .contentLength((long) body.length)
                        .metadata(Map.of(
                                "sha256", digest,
                                "vehicle-id", "vehicle-1",
                                "segment-id", "segment-1"))
                        .build());
        when(s3.getObject(any(GetObjectRequest.class))).thenReturn(
                new ResponseInputStream<>(
                        GetObjectResponse.builder().contentLength((long) body.length).build(),
                        AbortableInputStream.create(new ByteArrayInputStream(body))));

        CompleteUploadRequest request = CompleteUploadRequest.newBuilder()
                .setSegmentId("segment-1")
                .setUploadId("finished-upload")
                .setTStartUs(1_532_402_927_604_844L)
                .addParts(PartEtag.newBuilder().setPartNumber(1).setEtag("etag-1"))
                .build();
        var result = new Result<CompleteUploadResponse>();

        // When
        authenticated(() -> service.complete(request, result));

        // Then
        assertThat(result.error).isNull();
        assertThat(result.completed).isTrue();
        assertThat(result.value).isNotNull();
        assertThat(result.value.getVerified()).isTrue();
        assertThat(result.value.getSizeBytes()).isEqualTo(body.length);
        assertThat(result.value.getBlobUri()).contains("segment-1.mcap");
        verify(s3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void beginsValidMultipartUpload() throws Exception {
        // Given
        when(s3.createMultipartUpload(org.mockito.ArgumentMatchers
                .<Consumer<CreateMultipartUploadRequest.Builder>>any()))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        var signed = mock(PresignedUploadPartRequest.class);
        when(signed.url()).thenReturn(java.net.URI.create("https://storage.test/part").toURL());
        when(presigner.presignUploadPart(any(UploadPartPresignRequest.class))).thenReturn(signed);
        var result = new Result<BeginUploadResponse>();
        // When
        authenticated(() -> service.begin(begin().build(), result));
        // Then
        assertThat(result.error).isNull();
        assertThat(result.completed).isTrue();
        assertThat(result.value.getUploadId()).isEqualTo("upload-1");
        assertThat(result.value.getPartUrlsList()).extracting(p -> p.getPartNumber()).containsExactly(1, 2);
        verify(presigner, times(2)).presignUploadPart(any(UploadPartPresignRequest.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleansUpUnpublishedMultipartWhenSigningFails(boolean cleanupFails) {
        // Given
        when(s3.createMultipartUpload(org.mockito.ArgumentMatchers
                .<Consumer<CreateMultipartUploadRequest.Builder>>any()))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        var signingFailure = new IllegalStateException("secret signing detail");
        when(presigner.presignUploadPart(any(UploadPartPresignRequest.class))).thenThrow(signingFailure);
        if (cleanupFails) {
            when(s3.abortMultipartUpload(org.mockito.ArgumentMatchers
                    .<Consumer<AbortMultipartUploadRequest.Builder>>any())).thenThrow(s3Failure(503, "SlowDown"));
        }
        var result = new Result<BeginUploadResponse>();
        // When
        authenticated(() -> service.begin(begin().build(), result));
        // Then
        assertThat(Status.fromThrowable(result.error).getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(result.value).isNull();
        assertThat(result.completed).isFalse();
        assertThat(signingFailure.getSuppressed()).hasSize(cleanupFails ? 1 : 0);
        verify(s3).abortMultipartUpload(org.mockito.ArgumentMatchers
                .<Consumer<AbortMultipartUploadRequest.Builder>>any());
    }

    static Stream<Arguments> invalidBegins() {
        return Stream.of(
                Arguments.of("non-hex checksum", begin().setSha256("z".repeat(64)).build()),
                Arguments.of("short checksum", begin().setSha256("abc").build()),
                Arguments.of("empty object", begin().setSizeBytes(0).build()),
                Arguments.of("zero part size", begin().setPartSizeBytes(0).build()),
                Arguments.of("negative part size", begin().setPartSizeBytes(-1).build()),
                Arguments.of("oversized part", begin().setPartSizeBytes(6L << 30).build()),
                Arguments.of("too many parts without overflow", begin().setSizeBytes(Long.MAX_VALUE).build()),
                Arguments.of("invalid time range", begin().setTEndUs(1).build()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidBegins")
    void rejectsInvalidBeginBeforeStorageCall(String reason, BeginUploadRequest request) {
        // Given
        var result = new Result<BeginUploadResponse>();
        // When
        authenticated(() -> service.begin(request, result));
        // Then
        assertThat(Status.fromThrowable(result.error).getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.value).isNull();
        verifyNoInteractions(s3, presigner);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, 1001})
    void rejectsOutOfRangeRefreshPart(int number) {
        // Given
        var request = RefreshUrlsRequest.newBuilder().setSegmentId("segment-1")
                .setUploadId("upload-1").setTStartUs(1_532_402_927_604_844L)
                .addPartNumbers(number).build();
        var result = new Result<BeginUploadResponse>();
        // When
        authenticated(() -> service.refresh(request, result));
        // Then
        assertThat(Status.fromThrowable(result.error).getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        verifyNoInteractions(s3, presigner);
    }

    static Stream<Arguments> invalidCompletions() {
        return Stream.of(
                Arguments.of("duplicate numbers", List.of(part(1, "a"), part(1, "b"))),
                Arguments.of("zero number", List.of(part(0, "a"))),
                Arguments.of("out of range", List.of(part(1001, "a"))),
                Arguments.of("empty ETag", List.of(part(1, " "))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCompletions")
    void rejectsInvalidCompleteBeforeStorageCall(String reason, List<PartEtag> parts) {
        // Given
        var request = CompleteUploadRequest.newBuilder().setSegmentId("segment-1")
                .setUploadId("upload-1").setTStartUs(1_532_402_927_604_844L)
                .addAllParts(parts).build();
        var result = new Result<CompleteUploadResponse>();
        // When
        authenticated(() -> service.complete(request, result));
        // Then
        assertThat(Status.fromThrowable(result.error).getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        verifyNoInteractions(s3, presigner);
    }

    static Stream<Arguments> storageFailures() {
        return Stream.of(
                Arguments.of(s3Failure(503, "SlowDown"), Status.Code.UNAVAILABLE),
                Arguments.of(s3Failure(429, "TooManyRequests"), Status.Code.UNAVAILABLE),
                Arguments.of(s3Failure(404, "NoSuchUpload"), Status.Code.NOT_FOUND),
                Arguments.of(s3Failure(400, "InvalidPart"), Status.Code.INVALID_ARGUMENT),
                Arguments.of(s3Failure(403, "AccessDenied"), Status.Code.FAILED_PRECONDITION),
                Arguments.of(SdkClientException.create("secret credential detail"), Status.Code.INTERNAL),
                Arguments.of(SdkClientException.create("transport", new java.io.IOException("secret")),
                        Status.Code.UNAVAILABLE),
                Arguments.of(new IllegalStateException("secret internal detail"), Status.Code.INTERNAL));
    }

    @ParameterizedTest(name = "storage error → {1}")
    @MethodSource("storageFailures")
    void classifiesStorageErrorsWithoutLeakingExceptionMessage(RuntimeException failure, Status.Code expected) {
        // Given
        when(s3.abortMultipartUpload(org.mockito.ArgumentMatchers
                .<Consumer<AbortMultipartUploadRequest.Builder>>any())).thenThrow(failure);
        var request = AbortUploadRequest.newBuilder().setSegmentId("segment-1")
                .setUploadId("upload-1").setTStartUs(1_532_402_927_604_844L).build();
        var result = new Result<AbortUploadResponse>();
        // When
        authenticated(() -> service.abort(request, result));
        // Then
        var status = Status.fromThrowable(result.error);
        assertThat(status.getCode()).isEqualTo(expected);
        assertThat(status.getDescription()).doesNotContain("secret");
        assertThat(result.value).isNull();
    }

    private static BeginUploadRequest.Builder begin() {
        return BeginUploadRequest.newBuilder().setSizeBytes(10L << 20).setPartSizeBytes(5L << 20)
                .setSha256("a".repeat(64)).setTStartUs(1_532_402_927_604_844L)
                .setTEndUs(1_532_402_928_604_844L);
    }

    private static PartEtag part(int number, String etag) {
        return PartEtag.newBuilder().setPartNumber(number).setEtag(etag).build();
    }

    private static S3Exception s3Failure(int status, String code) {
        return (S3Exception) S3Exception.builder().statusCode(status)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code)
                        .errorMessage("secret storage detail").build()).build();
    }

    private static void authenticated(Runnable action) {
        Context.current().withValue(PeerIdentityInterceptor.IDENTITY,
                new VehicleIdentity("vehicle-1", "spiffe://fleetsentinel/vehicle/vehicle-1")).run(action);
    }

    private static final class Result<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private boolean completed;
        @Override public void onNext(T value) { this.value = value; }
        @Override public void onError(Throwable error) { this.error = error; }
        @Override public void onCompleted() { completed = true; }
    }
}
