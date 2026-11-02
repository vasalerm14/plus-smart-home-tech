package ru.yandex.practicum.telemetry.collector.handler.hub;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.service.EventService;

import java.time.Instant;

@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {

    private final EventService eventService;

    protected abstract T mapToAvro(HubEventProto event);

    @Override
    public void handle(HubEventProto event) {
        if (event.getPayloadCase() != getMessageType()) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getPayloadCase());
        }

        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(toInstant(event))
                .setPayload(mapToAvro(event))
                .build();

        eventService.collectHubEvent(eventAvro);
    }

    private Instant toInstant(HubEventProto event) {
        if (!event.hasTimestamp()) {
            return Instant.now();
        }
        Timestamp timestamp = event.getTimestamp();
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
