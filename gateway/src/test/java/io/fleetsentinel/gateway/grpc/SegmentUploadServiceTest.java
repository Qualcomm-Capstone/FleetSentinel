package io.fleetsentinel.gateway.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import io.fleetsentinel.gateway.identity.PeerIdentityInterceptor;
import io.fleetsentinel.gateway.identity.VehicleIdentity;
import io.fleetsentinel.gateway.proto.CompleteUploadRequest;
import io.fleetsentinel.gateway.proto.CompleteUploadResponse;
import io.fleetsentinel.gateway.proto.PartEtag;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

    @Test
    @DisplayName("이미 성공한 Complete 재호출은 최종 객체를 검증해 같은 성공으로 응답한다")
    void completeIsIdempotentAfterReplyLoss() throws Exception {
        byte[] body = "verified-object".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String digest = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(body));

        S3Client s3 = mock(S3Client.class);
        S3Exception noSuchUpload = (S3Exception) S3Exception.builder()
                .statusCode(404)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("NoSuchUpload")
                        .errorMessage("upload already completed")
                        .build())
                .build();
        when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenThrow(noSuchUpload);
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

        ObjectStoreProperties props = new ObjectStoreProperties();
        props.setBucket("bucket");
        SegmentUploadService service = new SegmentUploadService(
                s3, mock(S3Presigner.class), props, new SimpleMeterRegistry());
        CompleteUploadRequest request = CompleteUploadRequest.newBuilder()
                .setSegmentId("segment-1")
                .setUploadId("finished-upload")
                .setTStartUs(1_532_402_927_604_844L)
                .addParts(PartEtag.newBuilder().setPartNumber(1).setEtag("etag-1"))
                .build();
        AtomicReference<CompleteUploadResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        Context.current().withValue(
                PeerIdentityInterceptor.IDENTITY,
                new VehicleIdentity("vehicle-1", "spiffe://fleetsentinel/vehicle/vehicle-1"))
                .run(() -> service.complete(request, new StreamObserver<>() {
                    @Override
                    public void onNext(CompleteUploadResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable value) {
                        error.set(value);
                    }

                    @Override
                    public void onCompleted() {
                    }
                }));

        assertThat(error.get()).isNull();
        assertThat(response.get()).isNotNull();
        assertThat(response.get().getVerified()).isTrue();
        assertThat(response.get().getSizeBytes()).isEqualTo(body.length);
        assertThat(response.get().getBlobUri()).contains("segment-1.mcap");
        verify(s3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3).headObject(any(HeadObjectRequest.class));
    }
}
