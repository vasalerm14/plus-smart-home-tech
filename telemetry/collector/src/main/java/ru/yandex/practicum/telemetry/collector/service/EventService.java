package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {

    private final Producer<String, SpecificRecordBase> producer;

    @Value("${collector.kafka.topics.sensors}")
    private String sensorsTopic;

    @Value("${collector.kafka.topics.hubs}")
    private String hubsTopic;

    public void collectSensorEvent(SensorEventAvro event) {
        send(sensorsTopic, event.getHubId(), event);
    }

    public void collectHubEvent(HubEventAvro event) {
        send(hubsTopic, event.getHubId(), event);
    }

    private void send(String topic, String key, SpecificRecordBase value) {
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки события в топик {}", topic, exception);
            }
        });
    }
}
