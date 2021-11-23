package io.deephaven.engine.table.impl.by;

public interface ReaggregationIterativeOperator<T, STATE> {
    Class<?> getStateType();

    Class<?> getFinalResultType();

    T currentValue(STATE state);

    T prev(STATE state);
}
