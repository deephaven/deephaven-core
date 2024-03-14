//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.api.SortColumn;
import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see TableOperations#sort(Collection)
 */
@Immutable
@NodeStyle
public abstract class SortTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableSortTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<SortColumn> columns();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {

        Builder parent(TableSpec parent);

        Builder addColumns(SortColumn column);

        Builder addColumns(SortColumn... column);

        Builder addAllColumns(Iterable<? extends SortColumn> column);

        SortTable build();
    }
}
