package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfigProperties;
import ru.yandex.practicum.telemetry.analyzer.service.HubRouterClient;
import ru.yandex.practicum.telemetry.analyzer.service.ScenarioAnalyzer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioAnalyzer scenarioAnalyzer;
    private final HubRouterClient hubRouterClient;
    private final String topic;
    private final Duration pollTimeout;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public SnapshotProcessor(Consumer<String, SensorsSnapshotAvro> consumer,
                             ScenarioAnalyzer scenarioAnalyzer,
                             HubRouterClient hubRouterClient,
                             KafkaConfigProperties properties) {
        this.consumer = consumer;
        this.scenarioAnalyzer = scenarioAnalyzer;
        this.hubRouterClient = hubRouterClient;
        this.topic = properties.snapshots().topic();
        this.pollTimeout = properties.snapshots().pollTimeout();
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handleSnapshot(record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                if (!records.isEmpty()) {
                    consumer.commitAsync(currentOffsets, null);
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки снапшотов", e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Закрываем консьюмер снапшотов");
                consumer.close();
            }
        }
    }

    private void handleSnapshot(SensorsSnapshotAvro snapshot) {
        try {
            scenarioAnalyzer.analyze(snapshot).forEach(hubRouterClient::send);
        } catch (Exception e) {
            log.error("Ошибка анализа снапшота хаба {}", snapshot.getHubId(), e);
        }
    }
}
