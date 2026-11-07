package io.fleetsentinel.gateway.clip;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter @Setter
@ConfigurationProperties("fleetsentinel.clips.mqtt")
public class ClipMqttProperties {
    private boolean enabled = true;
    private String uri = "ssl://localhost:8883";
    private String clientId = "clip-service";
    private java.nio.file.Path pkiDirectory = java.nio.file.Path.of("./mqtt-pki");
}
