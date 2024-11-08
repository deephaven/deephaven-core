//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An {@link Aggregation aggregation} that provides a single output column with the number of rows in each aggregation
 * group.
 *
 * @see io.deephaven.api.TableOperations#countBy
 */
@Immutable
@SimpleStyle
public abstract class Count implements Aggregation {
    /**
     * The types of counts that can be performed.
     */
    public enum AggCountType {
        ALL, NON_NULL, NULL, NEGATIVE, POSITIVE, ZERO, NAN, INFINITE, FINITE
    }

    public static Count of(String column) {
        return of(ColumnName.of(column), AggCountType.ALL);
    }

    public static Count of(ColumnName column) {
        return of(column, AggCountType.ALL);
    }

    public static Count of(String column, AggCountType countType) {
        return of(ColumnName.of(column), countType);
    }

    public static Count of(ColumnName column, AggCountType countType) {
        return ImmutableCount.of(column, countType);
    }

    /**
     * The name of the input column from which to count values.
     */
    @Parameter
    public abstract ColumnName column();

    /**
     * The name of the input column from which to count values.
     */
    @Parameter
    public abstract AggCountType countType();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
