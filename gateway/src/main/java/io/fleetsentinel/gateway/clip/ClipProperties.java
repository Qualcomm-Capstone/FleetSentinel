package io.fleetsentinel.gateway.clip;

import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties("fleetsentinel.clips")
public class ClipProperties {
    private boolean enabled = true;
    private boolean workerEnabled = true;
    @Min(60) private int leaseSeconds = 180;
}
