/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.api.agg.Aggregation;

import java.util.Map;

/**
 * Rollup-specific {@link Aggregation aggregations}.
 */
public interface RollupAggregation extends Aggregation {

    static RollupAggregation nullColumns(String name, Class<?> type) {
        return NullColumns.of(name, type);
    }

    static RollupAggregation nullColumns(Map<String, Class<?>> resultColumns) {
        return NullColumns.from(resultColumns);
    }

    <V extends Aggregation.Visitor> V walk(V visitor);

    <V extends Visitor> V walk(V visitor);

    interface Visitor extends Aggregation.Visitor {
        void visit(NullColumns nullColumns);
    }
}
