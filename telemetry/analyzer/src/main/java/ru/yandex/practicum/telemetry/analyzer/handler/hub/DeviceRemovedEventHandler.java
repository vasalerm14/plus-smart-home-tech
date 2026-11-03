package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Component
@RequiredArgsConstructor
public class DeviceRemovedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public Class<?> getPayloadType() {
        return DeviceRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String sensorId = ((DeviceRemovedEventAvro) event.getPayload()).getId();
        sensorRepository.findByIdAndHubId(sensorId, event.getHubId()).ifPresent(sensor -> {
            scenarioRepository.findByHubId(event.getHubId()).forEach(scenario -> {
                scenario.getConditions().remove(sensorId);
                scenario.getActions().remove(sensorId);
            });
            sensorRepository.delete(sensor);
        });
    }
}
