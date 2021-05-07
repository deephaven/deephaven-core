package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;

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
