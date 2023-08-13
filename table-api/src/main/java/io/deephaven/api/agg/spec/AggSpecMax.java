/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SingletonStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the maximum value in the input column for each group. Only works for numeric or
 * {@link Comparable} input types.
 *
 * @see TableOperations#maxBy
 */
@Immutable
@SingletonStyle
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
