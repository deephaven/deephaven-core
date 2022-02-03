package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the variance of the input column values for each group. Only works for numeric
 * input types.
 *
 * @see TableOperations#varBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecVar extends AggSpecEmptyBase {

    public static AggSpecVar of() {
        return ImmutableAggSpecVar.of();
    }

    @Override
    public final String description() {
        return "variance";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
