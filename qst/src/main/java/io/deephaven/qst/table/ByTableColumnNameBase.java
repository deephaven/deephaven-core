/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;

import java.util.List;

public abstract class ByTableColumnNameBase extends TableBase implements SingleParentTable {

    public abstract TableSpec parent();

    public abstract List<ColumnName> groupByColumns();

    interface Builder<BY extends ByTableColumnNameBase, SELF extends Builder<BY, SELF>> {
        SELF parent(TableSpec parent);

        SELF addGroupByColumns(ColumnName element);

        SELF addGroupByColumns(ColumnName... elements);

        SELF addAllGroupByColumns(Iterable<? extends ColumnName> elements);

        BY build();
    }
}
