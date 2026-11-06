package ru.yandex.practicum.telemetry.analyzer.model;

import java.util.function.BiPredicate;

public enum ConditionOperation {
    EQUALS((actual, expected) -> actual.intValue() == expected.intValue()),
    GREATER_THAN((actual, expected) -> actual > expected),
    LOWER_THAN((actual, expected) -> actual < expected);

    private final BiPredicate<Integer, Integer> predicate;

    ConditionOperation(BiPredicate<Integer, Integer> predicate) {
        this.predicate = predicate;
    }

    public boolean test(Integer actual, Integer expected) {
        return predicate.test(actual, expected);
    }
}
