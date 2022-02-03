package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that freezes the first value for each group and ignores subsequent changes. When groups are
 * removed, the corresponding output row is removed. When groups are re-added (on a subsequent update cycle), the newly
 * added value is then frozen.
 * 
 * @implNote Only one row per group is allowed in the output, because the operation has no way to determine which row to
 *           freeze otherwise. This is a constraint on the input data.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecFreeze extends AggSpecEmptyBase {

    public static AggSpecFreeze of() {
        return ImmutableAggSpecFreeze.of();
    }

    @Override
    public final String description() {
        return "freeze";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
