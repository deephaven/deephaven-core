/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@NodeStyle
public abstract class DropColumnsTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableDropColumnsTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<ColumnName> dropColumns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {
        Builder parent(TableSpec parent);

        Builder addDropColumns(ColumnName element);

        Builder addDropColumns(ColumnName... elements);

        Builder addAllDropColumns(Iterable<? extends ColumnName> elements);

        DropColumnsTable build();
    }
}
