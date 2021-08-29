package io.deephaven.engine.v2.by;

import io.deephaven.engine.v2.sources.ColumnSource;

import java.util.Map;

class StaticColumnSourceTransformer implements AggregationContextTransformer {
    private final String name;
    private final ColumnSource<?> columnSource;

    StaticColumnSourceTransformer(String name, ColumnSource<?> columnSource) {
        this.name = name;
        this.columnSource = columnSource;
    }

    @Override
    public void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {
        resultColumns.put(name, columnSource);
    }
}
