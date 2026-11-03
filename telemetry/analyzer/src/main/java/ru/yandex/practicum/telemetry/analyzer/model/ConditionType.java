package ru.yandex.practicum.telemetry.analyzer.model;

import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.util.Optional;
import java.util.function.Function;

public enum ConditionType {
    MOTION(data -> data instanceof MotionSensorAvro motion ? toInt(motion.getMotion()) : null),
    LUMINOSITY(data -> data instanceof LightSensorAvro light ? light.getLuminosity() : null),
    SWITCH(data -> data instanceof SwitchSensorAvro switchSensor ? toInt(switchSensor.getState()) : null),
    TEMPERATURE(data -> switch (data) {
        case ClimateSensorAvro climate -> climate.getTemperatureC();
        case TemperatureSensorAvro temperature -> temperature.getTemperatureC();
        default -> null;
    }),
    CO2LEVEL(data -> data instanceof ClimateSensorAvro climate ? climate.getCo2Level() : null),
    HUMIDITY(data -> data instanceof ClimateSensorAvro climate ? climate.getHumidity() : null);

    private final Function<Object, Integer> valueExtractor;

    ConditionType(Function<Object, Integer> valueExtractor) {
        this.valueExtractor = valueExtractor;
    }

    public Optional<Integer> extractValue(Object sensorData) {
        return Optional.ofNullable(valueExtractor.apply(sensorData));
    }

    private static Integer toInt(boolean value) {
        return value ? 1 : 0;
    }
}
