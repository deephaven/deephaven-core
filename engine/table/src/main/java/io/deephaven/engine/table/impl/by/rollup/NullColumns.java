package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

import java.util.Map;

/**
 * {@link RollupAggregation} that allows columns to be nulled-out at higher aggregation levels.
 */
@Value.Immutable
@BuildableStyle
public abstract class NullColumns extends RollupAggregationBase {

    public static Builder builder() {
        return ImmutableNullColumns.builder();
    }

    public abstract Map<String, Class<?>> resultColumns();

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

        Builder putResultColumns(String name, Class<?> type);

        Builder putResultColumns(Map<String, Class<?>> resultColumns);

        NullColumns build();
    }
}
