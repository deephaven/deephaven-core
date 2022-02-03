package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the minimum value in the input column for each group. Only works for numeric or
 * {@link Comparable} input types.
 *
 * @see TableOperations#minBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecMin extends AggSpecEmptyBase {

    public static AggSpecMin of() {
        return ImmutableAggSpecMin.of();
    }

    @Override
    public final String description() {
        return "min";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
