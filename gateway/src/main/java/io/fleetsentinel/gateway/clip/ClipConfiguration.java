package io.fleetsentinel.gateway.clip;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ClipProperties.class, ClipMqttProperties.class})
class ClipConfiguration { }
