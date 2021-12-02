package io.deephaven.qst.table;

import io.deephaven.api.Selectable;

import java.util.List;

public abstract class ByTableBase extends TableBase implements SingleParentTable {

    public abstract TableSpec parent();

    public abstract List<Selectable> groupByColumns();

    interface Builder<BY extends ByTableBase, SELF extends Builder<BY, SELF>> {
        SELF parent(TableSpec parent);

        SELF addGroupByColumns(Selectable element);

        SELF addGroupByColumns(Selectable... elements);

        SELF addAllGroupByColumns(Iterable<? extends Selectable> elements);

        BY build();
    }
}
