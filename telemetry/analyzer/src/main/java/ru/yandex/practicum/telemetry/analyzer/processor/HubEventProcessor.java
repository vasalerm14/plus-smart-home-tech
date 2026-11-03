package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfigProperties;
import ru.yandex.practicum.telemetry.analyzer.handler.hub.HubEventHandler;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final Map<Class<?>, HubEventHandler> handlers;
    private final String topic;
    private final Duration pollTimeout;

    public HubEventProcessor(Consumer<String, HubEventAvro> consumer,
                             Set<HubEventHandler> handlers,
                             KafkaConfigProperties properties) {
        this.consumer = consumer;
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getPayloadType, Function.identity()));
        this.topic = properties.hubs().topic();
        this.pollTimeout = properties.hubs().pollTimeout();
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    handleEvent(record.value());
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хабов", e);
        } finally {
            log.info("Закрываем консьюмер событий хабов");
            consumer.close();
        }
    }

    private void handleEvent(HubEventAvro event) {
        HubEventHandler handler = handlers.get(event.getPayload().getClass());
        if (handler == null) {
            log.warn("Не найден обработчик для события хаба {}", event.getPayload().getClass().getSimpleName());
            return;
        }

        try {
            handler.handle(event);
        } catch (Exception e) {
            log.error("Ошибка обработки события хаба {}", event, e);
        }
    }
}
