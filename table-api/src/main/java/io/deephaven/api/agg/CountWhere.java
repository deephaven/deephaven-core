//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
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
public abstract class CountWhere implements Aggregation {

    public static CountWhere of(String name, String... filters) {
        return of(ColumnName.of(name), Filter.and(Filter.from(filters)));
    }

    public static CountWhere of(String name, Filter filter) {
        return of(ColumnName.of(name), filter);
    }

    public static CountWhere of(ColumnName name, Filter filter) {
        return ImmutableCountWhere.of(name, filter);
    }

    @Parameter
    public abstract ColumnName column();

    @Parameter
    public abstract Filter filter();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
