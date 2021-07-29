package io.deephaven.qst.table;

import io.deephaven.api.Selectable;

import java.util.List;

public interface SelectableTable extends SingleParentTable {

    List<Selectable> columns();

    interface Builder<S extends SelectableTable, SELF extends Builder<S, SELF>> {
        SELF parent(TableSpec parent);

        SELF addColumns(Selectable element);

        SELF addColumns(Selectable... elements);

        SELF addAllColumns(Iterable<? extends Selectable> elements);

        S build();
    }
}
