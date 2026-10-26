package io.fleetsentinel.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

class ObjectStoreConfigTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
            .withUserConfiguration(ObjectStoreConfig.class);

    @Test
    void usesDefaultCredentialChainWithoutResolvingExternalCredentials() {
        // Given / When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed().hasSingleBean(AwsCredentialsProvider.class);
            var credentials = context.getBean(AwsCredentialsProvider.class);
            assertThat(credentials).isInstanceOf(DefaultCredentialsProvider.class);
            assertThat(context.getBean(S3Client.class).serviceClientConfiguration().credentialsProvider())
                    .isSameAs(credentials);
            assertThat(context).hasSingleBean(S3Presigner.class);
        });
    }

    @ParameterizedTest(name = "local profile={0}")
    @ValueSource(booleans = {false, true})
    void usesStaticKeysFromLocalProfileOrDeploymentSettings(boolean localProfile) {
        // Given
        var local = runner.withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("MINIO_ACCESS_KEY=test-access", "MINIO_SECRET_KEY=test-secret",
                        "MINIO_ENDPOINT=http://localhost:9000");
        local = localProfile ? local.withPropertyValues("spring.profiles.active=local")
                : local.withPropertyValues("OBJECT_STORE_CREDENTIALS_MODE=static",
                        "OBJECT_STORE_PATH_STYLE_ACCESS=true");
        // When
        local.run(context -> {
            // Then
            assertThat(context).hasNotFailed();
            var credentials = context.getBean(AwsCredentialsProvider.class);
            assertThat(credentials).isInstanceOf(StaticCredentialsProvider.class);
            assertThat(credentials.resolveCredentials().accessKeyId()).isEqualTo("test-access");
            assertThat(context.getBean(ObjectStoreProperties.class).isPathStyleAccess()).isTrue();
            assertThat(context.getBean(S3Client.class).serviceClientConfiguration().credentialsProvider())
                    .isSameAs(credentials);
        });
    }
}
