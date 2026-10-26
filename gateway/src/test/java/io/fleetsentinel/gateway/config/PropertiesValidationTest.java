package io.fleetsentinel.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class PropertiesValidationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
            .withUserConfiguration(Config.class);

    @Test
    void acceptsDefaults() {
        // Given / When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed();
        });
    }

    @ParameterizedTest(name = "invalid setting: {0}")
    @ValueSource(strings = {
            "gateway.max-inflight=0", "gateway.ack.every-n=0", "gateway.ack.every=0ms",
            "gateway.ack.every=-1ms", "gateway.ack.every=1ns", "gateway.topics.records=",
            "object-store.bucket=", "object-store.region=", "object-store.key-prefix=../escape",
            "object-store.max-parts=0", "object-store.max-parts=10001",
            "object-store.min-part-size-bytes=1", "object-store.presign-ttl=0s",
            "object-store.presign-ttl=8d", "object-store.endpoint=invalid-uri",
            "object-store.credentials-mode=static", "object-store.access-key=partial-key"
    })
    void rejectsInvalidSettingsAtStartup(String setting) {
        // Given / When
        runner.withPropertyValues("fleetsentinel." + setting).run(context -> {
            // Then
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({GatewayProperties.class, ObjectStoreProperties.class})
    static class Config {}
}
