package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the maximum value in the input column for each group. Only works for numeric or
 * {@link Comparable} input types.
 *
 * @see TableOperations#maxBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecMax extends AggSpecEmptyBase {

    public static AggSpecMax of() {
        return ImmutableAggSpecMax.of();
    }

    @Override
    public final String description() {
        return "max";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
