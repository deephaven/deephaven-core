//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.ColumnDefinition;

import java.util.Map;

/**
 * Rollup-specific {@link Aggregation aggregations}.
 */
public interface RollupAggregation extends Aggregation {

    static RollupAggregation nullColumns(final ColumnDefinition<?> type) {
        return NullColumns.of(type);
    }

    static RollupAggregation nullColumns(final Iterable<ColumnDefinition<?>> resultColumns) {
        return NullColumns.from(resultColumns);
    }

    <V extends Aggregation.Visitor> V walk(V visitor);

    <V extends Visitor> V walk(V visitor);

    interface Visitor extends Aggregation.Visitor {
        void visit(NullColumns nullColumns);
    }
}
