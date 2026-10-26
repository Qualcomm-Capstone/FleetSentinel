package io.fleetsentinel.api.config;

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

class ApiPropertiesTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
            .withUserConfiguration(Config.class);

    @Test
    void acceptsDefaults() {
        // Given / When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed().hasSingleBean(ApiProperties.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"push-interval-ms=0", "push-interval-ms=-1",
            "max-records-per-push=0", "max-records-per-push=-1"})
    void rejectsInvalidSettingsAtStartup(String setting) {
        // Given / When
        runner.withPropertyValues("fleetsentinel.api." + setting).run(context -> {
            // Then
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ApiProperties.class)
    static class Config {}
}
