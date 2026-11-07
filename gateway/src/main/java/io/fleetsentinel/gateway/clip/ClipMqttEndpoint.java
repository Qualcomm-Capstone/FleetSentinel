package io.fleetsentinel.gateway.clip;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.pem.PemSslStoreBundle;
import org.springframework.boot.ssl.pem.PemSslStoreDetails;
import org.springframework.stereotype.Component;

/** Bounded transport adapter; DB state and edge retries survive lost MQTT replies. */
@Component @RequiredArgsConstructor @Slf4j
@ConditionalOnProperty(prefix = "fleetsentinel.clips", name = {"enabled", "mqtt.enabled"}, havingValue = "true", matchIfMissing = true)
public class ClipMqttEndpoint {
    private final ClipMqttHandler handler;
    private final ClipMqttProperties properties;
    private final ThreadPoolExecutor requests = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(16), Thread.ofPlatform().name("clip-mqtt-request-", 0).factory());
    private final ScheduledExecutorService connector = Executors.newSingleThreadScheduledExecutor();
    private MqttAsyncClient client;
    private MqttConnectOptions options;

    @PostConstruct void start() throws MqttException {
        if (!properties.getUri().startsWith("ssl://")) throw new IllegalArgumentException("clip MQTT requires TLS");
        client = new MqttAsyncClient(properties.getUri(), properties.getClientId(), new MemoryPersistence());
        options = new MqttConnectOptions();
        // Only load this feature's PEM material when MQTT control is enabled.
        var directory = properties.getPkiDirectory().toAbsolutePath();
        String name = properties.getClientId();
        if (!name.matches("[a-zA-Z0-9_-]{1,80}")) throw new IllegalArgumentException("invalid MQTT client identity");
        var keys = PemSslStoreDetails.forCertificate(directory.resolve(name + ".crt").toUri().toString())
                .withPrivateKey(directory.resolve(name + ".key").toUri().toString());
        var trust = PemSslStoreDetails.forCertificate(directory.resolve("ca.crt").toUri().toString());
        options.setSocketFactory(SslBundle.of(new PemSslStoreBundle(keys, trust)).createSslContext().getSocketFactory());
        options.setHttpsHostnameVerificationEnabled(true);
        options.setCleanSession(true); // Application retries are authoritative, including broker restart.
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(false);
        options.setMaxInflight(20);
        client.setCallback(new MqttCallbackExtended() {
            public void connectComplete(boolean reconnect, String uri) { }
            public void connectionLost(Throwable cause) { log.warn("Clip MQTT disconnected; edge retries remain durable"); }
            public void deliveryComplete(IMqttDeliveryToken token) { }
            public void messageArrived(String topic, MqttMessage message) {
                if (message.isRetained() || message.getPayload().length > ClipMqttHandler.MAX_BYTES) return;
                byte[] payload = message.getPayload().clone();
                try { requests.execute(() -> handler.handle(topic, payload, false).ifPresent(ClipMqttEndpoint.this::reply)); }
                catch (RejectedExecutionException full) { /* Edge timeout/backoff retries; do not block network callbacks. */ }
            }
        });
        connector.scheduleWithFixedDelay(() -> {
            if (!client.isConnected()) {
                try {
                    client.connect(options).waitForCompletion(10000);
                    client.subscribe("$share/clip-control/fleet/v1/+/clip/request", 1).waitForCompletion(10000);
                } catch (MqttException e) { log.debug("Clip MQTT connection deferred"); disconnect(); }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void reply(ClipMqttHandler.Reply reply) {
        try { client.publish(reply.topic(), reply.payload(), 1, false).waitForCompletion(10000); }
        catch (MqttException e) { log.debug("Clip MQTT response lost; operation can be replayed"); }
    }
    private void disconnect() {
        try { client.disconnectForcibly(0, 1000, false); } catch (MqttException ignored) { }
    }
    @PreDestroy void stop() {
        connector.shutdownNow(); requests.shutdownNow();
        if (client != null) {
            disconnect();
            try { client.close(true); } catch (MqttException ignored) { }
        }
    }
}
