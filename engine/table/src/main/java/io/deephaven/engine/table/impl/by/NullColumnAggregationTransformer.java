/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

class NullColumnAggregationTransformer implements AggregationContextTransformer {

    private final Map<String, Class<?>> resultColumnTypes;

    NullColumnAggregationTransformer(@NotNull final Map<String, Class<?>> resultColumnTypes) {
        this.resultColumnTypes = resultColumnTypes;
    }

    @Override
    public void resultColumnFixup(@NotNull final Map<String, ColumnSource<?>> resultColumns) {
        final Map<String, ColumnSource<?>> savedColumns = new LinkedHashMap<>(resultColumns);
        resultColumns.clear();
        resultColumnTypes
                .forEach((key, value) -> resultColumns.put(key, NullValueColumnSource.getInstance(value, null)));
        resultColumns.putAll(savedColumns);
    }
}
