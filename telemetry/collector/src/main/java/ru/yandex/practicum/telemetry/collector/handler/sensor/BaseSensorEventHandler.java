package ru.yandex.practicum.telemetry.collector.handler.sensor;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.service.EventService;

import java.time.Instant;

@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    private final EventService eventService;

    protected abstract T mapToAvro(SensorEventProto event);

    @Override
    public void handle(SensorEventProto event) {
        if (event.getPayloadCase() != getMessageType()) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getPayloadCase());
        }

        SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(toInstant(event))
                .setPayload(mapToAvro(event))
                .build();

        eventService.collectSensorEvent(eventAvro);
    }

    private Instant toInstant(SensorEventProto event) {
        if (!event.hasTimestamp()) {
            return Instant.now();
        }
        Timestamp timestamp = event.getTimestamp();
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
