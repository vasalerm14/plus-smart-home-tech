package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ScenarioAnalyzer {

    private final ScenarioRepository scenarioRepository;

    @Transactional(readOnly = true)
    public List<DeviceActionRequest> analyze(SensorsSnapshotAvro snapshot) {
        return scenarioRepository.findByHubId(snapshot.getHubId()).stream()
                .filter(scenario -> isTriggered(scenario, snapshot.getSensorsState()))
                .flatMap(scenario -> scenario.getActions().entrySet().stream()
                        .map(action -> toRequest(snapshot.getHubId(), scenario.getName(),
                                action.getKey(), action.getValue())))
                .toList();
    }

    private boolean isTriggered(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        return !scenario.getConditions().isEmpty()
                && scenario.getConditions().entrySet().stream()
                .allMatch(condition -> isConditionMet(condition.getValue(), sensorsState.get(condition.getKey())));
    }

    private boolean isConditionMet(Condition condition, SensorStateAvro sensorState) {
        if (sensorState == null || condition.getValue() == null) {
            return false;
        }
        return Optional.ofNullable(sensorState.getData())
                .flatMap(condition.getType()::extractValue)
                .map(actual -> condition.getOperation().test(actual, condition.getValue()))
                .orElse(false);
    }

    private DeviceActionRequest toRequest(String hubId, String scenarioName, String sensorId, Action action) {
        DeviceActionProto.Builder actionProto = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(action.getType().name()));
        if (action.getValue() != null) {
            actionProto.setValue(action.getValue());
        }

        Instant now = Instant.now();
        return DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(actionProto)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano()))
                .build();
    }
}
