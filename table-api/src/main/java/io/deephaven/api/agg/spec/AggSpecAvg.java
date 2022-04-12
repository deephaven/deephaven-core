package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the arithmetic mean for each group. Only works with numeric input types.
 *
 * @see io.deephaven.api.TableOperations#avgBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecAvg extends AggSpecEmptyBase {

    public static AggSpecAvg of() {
        return ImmutableAggSpecAvg.of();
    }

    @Override
    public final String description() {
        return "average";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
