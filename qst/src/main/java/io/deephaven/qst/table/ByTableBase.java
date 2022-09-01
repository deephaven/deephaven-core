/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;

import java.util.List;

public abstract class ByTableBase extends TableBase {

    public abstract TableSpec parent();

    public abstract List<ColumnName> groupByColumns();

    interface Builder<BY extends ByTableBase, SELF extends Builder<BY, SELF>> {
        SELF parent(TableSpec parent);

        SELF addGroupByColumns(ColumnName element);

        SELF addGroupByColumns(ColumnName... elements);

        SELF addAllGroupByColumns(Iterable<? extends ColumnName> elements);

        BY build();
    }
}
