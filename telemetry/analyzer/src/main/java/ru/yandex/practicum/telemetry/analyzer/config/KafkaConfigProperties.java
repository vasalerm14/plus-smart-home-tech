package ru.yandex.practicum.telemetry.analyzer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

@ConfigurationProperties("analyzer.kafka")
public record KafkaConfigProperties(ConsumerSettings hubs, ConsumerSettings snapshots) {

    public record ConsumerSettings(String topic, Duration pollTimeout, Map<String, String> properties) {

        public Properties toProperties() {
            Properties config = new Properties();
            config.putAll(properties);
            return config;
        }
    }
}
