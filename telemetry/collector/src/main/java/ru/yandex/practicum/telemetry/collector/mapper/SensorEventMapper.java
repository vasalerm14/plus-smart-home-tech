package ru.yandex.practicum.telemetry.collector.mapper;

import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.ClimateSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.LightSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.MotionSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SwitchSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.TemperatureSensorEvent;

public final class SensorEventMapper {

    private SensorEventMapper() {
    }

    public static SensorEventAvro toAvro(SensorEvent event) {
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(toPayload(event))
                .build();
    }

    private static Object toPayload(SensorEvent event) {
        return switch (event) {
            case ClimateSensorEvent climate -> ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climate.getTemperatureC())
                    .setHumidity(climate.getHumidity())
                    .setCo2Level(climate.getCo2Level())
                    .build();
            case LightSensorEvent light -> LightSensorAvro.newBuilder()
                    .setLinkQuality(light.getLinkQuality())
                    .setLuminosity(light.getLuminosity())
                    .build();
            case MotionSensorEvent motion -> MotionSensorAvro.newBuilder()
                    .setLinkQuality(motion.getLinkQuality())
                    .setMotion(motion.isMotion())
                    .setVoltage(motion.getVoltage())
                    .build();
            case SwitchSensorEvent switchEvent -> SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build();
            case TemperatureSensorEvent temperature -> TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(temperature.getTemperatureC())
                    .setTemperatureF(temperature.getTemperatureF())
                    .build();
            default -> throw new IllegalArgumentException("Неизвестный тип события датчика: " + event.getType());
        };
    }
}
