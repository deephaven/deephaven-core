/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SingletonStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the last value in the input column for each group.
 *
 * @see TableOperations#lastBy
 */
@Immutable
@SingletonStyle
public abstract class AggSpecLast extends AggSpecEmptyBase {

    public static AggSpecLast of() {
        return ImmutableAggSpecLast.of();
    }

    @Override
    public final String description() {
        return "last";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
