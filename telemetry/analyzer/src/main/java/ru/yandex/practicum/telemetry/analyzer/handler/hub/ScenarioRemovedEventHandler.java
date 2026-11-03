package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;

    @Override
    public Class<?> getPayloadType() {
        return ScenarioRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String name = ((ScenarioRemovedEventAvro) event.getPayload()).getName();
        scenarioRepository.findByHubIdAndName(event.getHubId(), name)
                .ifPresent(scenarioRepository::delete);
    }
}
