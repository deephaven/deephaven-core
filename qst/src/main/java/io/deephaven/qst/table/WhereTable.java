//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see TableOperations#where(Filter)
 */
@Immutable
@NodeStyle
public abstract class WhereTable extends TableBase implements SingleParentTable {

    public static WhereTable of(TableSpec parent, Filter filter) {
        return ImmutableWhereTable.of(parent, filter);
    }

    @Parameter
    public abstract TableSpec parent();

    @Parameter
    public abstract Filter filter();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
