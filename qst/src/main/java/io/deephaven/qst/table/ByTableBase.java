package io.deephaven.qst.table;

import io.deephaven.api.Selectable;

import java.util.List;

public abstract class ByTableBase extends TableBase implements SingleParentTable {

    public abstract TableSpec parent();

    public abstract List<Selectable> columns();

    interface Builder<BY extends ByTableBase, SELF extends Builder<BY, SELF>> {
        SELF parent(TableSpec parent);

        SELF addColumns(Selectable element);

        SELF addColumns(Selectable... elements);

        SELF addAllColumns(Iterable<? extends Selectable> elements);

        BY build();
    }
}
