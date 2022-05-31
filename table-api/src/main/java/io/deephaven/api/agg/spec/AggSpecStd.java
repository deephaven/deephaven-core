package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the standard deviation of the input column values for each group. Only works
 * for numeric input types.
 *
 * @see TableOperations#stdBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecStd extends AggSpecEmptyBase {

    public static AggSpecStd of() {
        return ImmutableAggSpecStd.of();
    }

    @Override
    public final String description() {
        return "standard deviation";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
