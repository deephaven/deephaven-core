/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationOutputs;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A visitor to get the ordered output {@link ColumnName column names} for {@link Aggregation aggregations}, including
 * {@link RollupAggregation rollup aggregations}.
 */
public class RollupAggregationOutputs extends AggregationOutputs implements RollupAggregation.Visitor {

    public static Stream<ColumnName> of(Aggregation aggregation) {
        return aggregation.walk(new RollupAggregationOutputs()).getOut();
    }

    public static Stream<ColumnName> of(Collection<? extends Aggregation> aggregations) {
        return aggregations.stream().flatMap(RollupAggregationOutputs::of);
    }

    @Override
    public void visit(NullColumns nullColumns) {
        out = nullColumns.resultColumns().keySet().stream().map(ColumnName::of);
    }
}
