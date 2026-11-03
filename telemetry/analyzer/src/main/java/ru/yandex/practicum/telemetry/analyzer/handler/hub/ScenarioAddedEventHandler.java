package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.ActionType;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Override
    public Class<?> getPayloadType() {
        return ScenarioAddedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioAdded = (ScenarioAddedEventAvro) event.getPayload();
        String hubId = event.getHubId();

        if (!allSensorsRegistered(scenarioAdded, hubId)) {
            log.warn("Сценарий {} хаба {} ссылается на незарегистрированные датчики и не будет сохранён",
                    scenarioAdded.getName(), hubId);
            return;
        }

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioAdded.getName())
                .orElseGet(() -> {
                    Scenario newScenario = new Scenario();
                    newScenario.setHubId(hubId);
                    newScenario.setName(scenarioAdded.getName());
                    return newScenario;
                });

        scenario.getConditions().clear();
        scenarioAdded.getConditions()
                .forEach(condition -> scenario.getConditions().put(condition.getSensorId(), toCondition(condition)));

        scenario.getActions().clear();
        scenarioAdded.getActions()
                .forEach(action -> scenario.getActions().put(action.getSensorId(), toAction(action)));

        scenarioRepository.save(scenario);
    }

    private boolean allSensorsRegistered(ScenarioAddedEventAvro scenarioAdded, String hubId) {
        Set<String> sensorIds = new HashSet<>();
        scenarioAdded.getConditions().forEach(condition -> sensorIds.add(condition.getSensorId()));
        scenarioAdded.getActions().forEach(action -> sensorIds.add(action.getSensorId()));

        Set<String> registeredIds = sensorRepository.findAllById(sensorIds).stream()
                .filter(sensor -> hubId.equals(sensor.getHubId()))
                .map(Sensor::getId)
                .collect(Collectors.toSet());
        return registeredIds.containsAll(sensorIds);
    }

    private Condition toCondition(ScenarioConditionAvro conditionAvro) {
        Condition condition = new Condition();
        condition.setType(ConditionType.valueOf(conditionAvro.getType().name()));
        condition.setOperation(ConditionOperation.valueOf(conditionAvro.getOperation().name()));
        condition.setValue(toConditionValue(conditionAvro.getValue()));
        return condition;
    }

    private Integer toConditionValue(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue ? 1 : 0;
        }
        return (Integer) value;
    }

    private Action toAction(DeviceActionAvro actionAvro) {
        Action action = new Action();
        action.setType(ActionType.valueOf(actionAvro.getType().name()));
        action.setValue(actionAvro.getValue());
        return action;
    }
}
