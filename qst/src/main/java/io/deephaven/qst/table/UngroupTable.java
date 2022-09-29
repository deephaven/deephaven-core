/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@NodeStyle
public abstract class UngroupTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableUngroupTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<ColumnName> ungroupColumns();

    @Default
    public boolean nullFill() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    interface Builder {
        Builder parent(TableSpec parent);

        Builder addUngroupColumns(ColumnName element);

        Builder addUngroupColumns(ColumnName... elements);

        Builder addAllUngroupColumns(Iterable<? extends ColumnName> elements);

        Builder nullFill(boolean nullFill);

        UngroupTable build();
    }
}
