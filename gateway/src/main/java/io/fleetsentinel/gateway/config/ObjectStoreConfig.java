package io.fleetsentinel.gateway.config;

import java.net.URI;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** S3 클라이언트와 presigner가 같은 자격증명 provider를 사용한다. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ObjectStoreProperties.class)
public class ObjectStoreConfig {

    @Bean
    public AwsCredentialsProvider objectStoreCredentials(ObjectStoreProperties p) {
        if (p.getCredentialsMode() == ObjectStoreProperties.CredentialsMode.STATIC) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(p.getAccessKey(), p.getSecretKey()));
        }
        return DefaultCredentialsProvider.builder().build();
    }

    @Bean(destroyMethod = "close")
    public S3Client s3Client(ObjectStoreProperties p, AwsCredentialsProvider credentials) {
        var builder = S3Client.builder()
                .region(Region.of(p.getRegion()))
                .credentialsProvider(credentials)
                // MinIO는 가상 호스트 스타일을 기본 지원하지 않는다.
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(p.isPathStyleAccess())
                        .build());
        if (p.getEndpoint() != null && !p.getEndpoint().isBlank()) {
            builder.endpointOverride(URI.create(p.getEndpoint()));
        }
        return builder.build();
    }

    @Bean(destroyMethod = "close")
    public S3Presigner s3Presigner(ObjectStoreProperties p, AwsCredentialsProvider credentials) {
        var builder = S3Presigner.builder()
                .region(Region.of(p.getRegion()))
                .credentialsProvider(credentials)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(p.isPathStyleAccess())
                        .build());
        if (p.getEndpoint() != null && !p.getEndpoint().isBlank()) {
            builder.endpointOverride(URI.create(p.getEndpoint()));
        }
        return builder.build();
    }
}
