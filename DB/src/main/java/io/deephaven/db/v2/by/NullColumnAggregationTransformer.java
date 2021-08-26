package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.NullValueColumnSource;

import java.util.LinkedHashMap;
import java.util.Map;

public class NullColumnAggregationTransformer implements AggregationContextTransformer {
    final private Map<String, Class> resultColumnTypes;

    NullColumnAggregationTransformer(Map<String, Class> resultColumnTypes) {
        this.resultColumnTypes = resultColumnTypes;
    }

    @Override
    public void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {
        final Map<String, ColumnSource<?>> savedColumns = new LinkedHashMap<>(resultColumns);
        resultColumns.clear();
        // noinspection unchecked
        resultColumnTypes.forEach(
            (key, value) -> resultColumns.put(key, NullValueColumnSource.getInstance(value, null)));
        resultColumns.putAll(savedColumns);
    }
}
