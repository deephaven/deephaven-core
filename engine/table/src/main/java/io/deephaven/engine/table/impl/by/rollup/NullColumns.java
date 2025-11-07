//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.ColumnDefinition;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * {@link RollupAggregation} that allows columns to be nulled-out at higher aggregation levels.
 */
@Value.Immutable
@BuildableStyle
public abstract class NullColumns extends RollupAggregationBase {

    public static NullColumns of(ColumnDefinition<?> resultColumn) {
        return builder().addResultColumns(resultColumn).build();
    }

    public static NullColumns from(Iterable<? extends ColumnDefinition<?>> resultColumns) {
        return builder().addAllResultColumns(resultColumns).build();
    }

    public static Builder builder() {
        return ImmutableNullColumns.builder();
    }

    public abstract List<ColumnDefinition<?>> resultColumns();

    @Override
    public final <V extends RollupAggregation.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Value.Check
    final void checkNonEmpty() {
        if (resultColumns().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("%s should have at least one result column", NullColumns.class));
        }
    }

    public interface Builder {
        Builder addResultColumns(ColumnDefinition<?> type);

        Builder addAllResultColumns(Iterable<? extends ColumnDefinition<?>> resultColumns);

        NullColumns build();
    }
}
