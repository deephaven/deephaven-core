//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class NullColumnAggregationTransformer implements AggregationContextTransformer {

    private final List<ColumnDefinition<?>> resultColumnTypes;

    NullColumnAggregationTransformer(@NotNull final List<ColumnDefinition<?>> resultColumnTypes) {
        this.resultColumnTypes = resultColumnTypes;
    }

    @Override
    public void resultColumnFixup(@NotNull final Map<String, ColumnSource<?>> resultColumns) {
        final Map<String, ColumnSource<?>> savedColumns = new LinkedHashMap<>(resultColumns);
        resultColumns.clear();
        resultColumnTypes
                .forEach((cd) -> resultColumns.put(cd.getName(),
                        NullValueColumnSource.getInstance(cd.getDataType(), cd.getComponentType())));
        resultColumns.putAll(savedColumns);
    }
}
