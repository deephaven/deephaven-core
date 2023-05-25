/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

class StaticColumnSourceTransformer implements AggregationContextTransformer {

    private final String name;
    private final ColumnSource<?> columnSource;

    StaticColumnSourceTransformer(@NotNull final String name, @NotNull final ColumnSource<?> columnSource) {
        this.name = name;
        this.columnSource = columnSource;
    }

    @Override
    public void resultColumnFixup(@NotNull final Map<String, ColumnSource<?>> resultColumns) {
        resultColumns.put(name, columnSource);
    }
}
