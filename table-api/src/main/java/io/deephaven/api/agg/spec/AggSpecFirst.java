package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the first value in the input column for each group.
 *
 * @see TableOperations#firstBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecFirst extends AggSpecEmptyBase {

    public static AggSpecFirst of() {
        return ImmutableAggSpecFirst.of();
    }

    @Override
    public final String description() {
        return "first";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
