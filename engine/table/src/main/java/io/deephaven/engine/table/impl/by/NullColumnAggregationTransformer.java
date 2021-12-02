package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;

import java.util.LinkedHashMap;
import java.util.Map;

public class NullColumnAggregationTransformer implements AggregationContextTransformer {
    final private Map<String, Class<?>> resultColumnTypes;

    NullColumnAggregationTransformer(Map<String, Class<?>> resultColumnTypes) {
        this.resultColumnTypes = resultColumnTypes;
    }

    @Override
    public void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {
        final Map<String, ColumnSource<?>> savedColumns = new LinkedHashMap<>(resultColumns);
        resultColumns.clear();
        resultColumnTypes
                .forEach((key, value) -> resultColumns.put(key, NullValueColumnSource.getInstance(value, null)));
        resultColumns.putAll(savedColumns);
    }
}
