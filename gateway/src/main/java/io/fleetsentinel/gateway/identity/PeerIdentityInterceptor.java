package io.fleetsentinel.gateway.identity;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;

/**
 * 스트림이 열릴 때 신원을 확정한다. 여기를 통과하지 못하면 레코드가 Kafka에 닿지 않는다.
 *
 * <p>검사는 셋이다.
 *
 * <ol>
 *   <li>TLS peer 인증서가 있는가 — {@code client-auth=REQUIRE}가 이미 강제하지만, 설정이
 *       풀렸을 때 조용히 열리지 않도록 여기서 한 번 더 막는다
 *   <li>인증서 SAN URI가 발급 규약에 맞는가
 *   <li>차량이 주장한 {@code x-vehicle-id}가 인증서와 같은가
 * </ol>
 *
 * <p>3번이 없어도 시스템은 "동작"한다 — 신원을 인증서에서만 취하므로 주장은 무시하면 그만이다.
 * 그럼에도 대조하는 이유는 <b>불일치가 관측 가능해야</b> 하기 때문이다. 인증서를 잘못 심은
 * 차량과 남을 사칭하려는 차량은 같은 신호를 내고, 둘 다 조용히 넘기면 나중에 "왜 이 차량
 * 데이터가 없지"로 돌아온다.
 */
@Component
@GlobalServerInterceptor
@Slf4j
public class PeerIdentityInterceptor implements ServerInterceptor {

    /** 차량이 주장하는 ID. 인증서와 대조된다. */
    public static final Metadata.Key<String> VEHICLE_ID_CLAIM =
            Metadata.Key.of("x-vehicle-id", Metadata.ASCII_STRING_MARSHALLER);

    /** 이 스트림을 여는 WAL 세션의 boot_id. 신원이 아니라 세션 식별자다. */
    public static final Metadata.Key<String> BOOT_ID =
            Metadata.Key.of("x-boot-id", Metadata.ASCII_STRING_MARSHALLER);

    /** 확정된 신원. 서비스 구현은 여기서만 vehicleId를 읽는다. */
    public static final Context.Key<VehicleIdentity> IDENTITY = Context.key("fleetsentinel.identity");

    /** 이 스트림의 boot_id. */
    public static final Context.Key<String> BOOT = Context.key("fleetsentinel.bootId");

    private final Counter noCert;
    private final Counter badSan;
    private final Counter claimMismatch;
    private final Counter missingBootId;

    public PeerIdentityInterceptor(MeterRegistry meters) {
        this.noCert = rejection(meters, "no_peer_certificate");
        this.badSan = rejection(meters, "san_not_conformant");
        this.claimMismatch = rejection(meters, "vehicle_id_claim_mismatch");
        this.missingBootId = rejection(meters, "missing_boot_id");
    }

    private static Counter rejection(MeterRegistry meters, String reason) {
        return Counter.builder("fleetsentinel.gateway.stream.rejected")
                .description("신원 검사에서 거절된 스트림 수")
                .tag("reason", reason)
                .register(meters);
    }

    @Override
    public <Q, S> ServerCall.Listener<Q> interceptCall(
            ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {

        Optional<X509Certificate> peer = peerCertificate(call);
        if (peer.isEmpty()) {
            return deny(call, noCert, Status.UNAUTHENTICATED
                    .withDescription("client certificate required"));
        }

        Optional<VehicleIdentity> identity = VehicleIdentity.fromCertificate(peer.get());
        if (identity.isEmpty()) {
            log.warn("SAN이 발급 규약에 맞지 않는다: subject={}", peer.get().getSubjectX500Principal());
            return deny(call, badSan, Status.UNAUTHENTICATED
                    .withDescription("certificate has no conformant spiffe://fleetsentinel/vehicle/{id} SAN URI"));
        }

        String bootId = headers.get(BOOT_ID);
        if (bootId == null || bootId.isBlank()) {
            return deny(call, missingBootId, Status.INVALID_ARGUMENT
                    .withDescription("x-boot-id metadata required"));
        }

        String claim = headers.get(VEHICLE_ID_CLAIM);
        if (claim != null && !claim.equals(identity.get().vehicleId())) {
            // 조용히 인증서 값을 쓰지 않는다 — 사칭과 오설정을 구분할 유일한 신호다.
            log.warn("vehicle_id 주장이 인증서와 다르다: claimed={} cert={} boot={}",
                    claim, identity.get().vehicleId(), bootId);
            return deny(call, claimMismatch, Status.PERMISSION_DENIED
                    .withDescription("x-vehicle-id does not match client certificate identity"));
        }

        Context ctx = Context.current()
                .withValue(IDENTITY, identity.get())
                .withValue(BOOT, bootId);
        return Contexts.interceptCall(ctx, call, headers, next);
    }

    private static Optional<X509Certificate> peerCertificate(ServerCall<?, ?> call) {
        SSLSession ssl = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        if (ssl == null) {
            return Optional.empty();
        }
        try {
            var chain = ssl.getPeerCertificates();
            if (chain.length > 0 && chain[0] instanceof X509Certificate x509) {
                return Optional.of(x509);
            }
            return Optional.empty();
        } catch (SSLPeerUnverifiedException e) {
            return Optional.empty();
        }
    }

    private static <Q, S> ServerCall.Listener<Q> deny(
            ServerCall<Q, S> call, Counter counter, Status status) {
        counter.increment();
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {
        };
    }
}
