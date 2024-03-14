//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.ColumnName;
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
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder parent(TableSpec parent);

        Builder addDropColumns(ColumnName element);

        Builder addDropColumns(ColumnName... elements);

        Builder addAllDropColumns(Iterable<? extends ColumnName> elements);

        DropColumnsTable build();
    }
}
