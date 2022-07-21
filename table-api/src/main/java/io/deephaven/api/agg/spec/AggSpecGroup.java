/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs each group of input values as a Deephaven vector (io.deephaven.vector.Vector).
 *
 * @see io.deephaven.api.TableOperations#groupBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecGroup extends AggSpecEmptyBase {

    public static AggSpecGroup of() {
        return ImmutableAggSpecGroup.of();
    }

    @Override
    public final String description() {
        return "group";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
