package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventHandler {

    Class<?> getPayloadType();

    void handle(HubEventAvro event);
}
