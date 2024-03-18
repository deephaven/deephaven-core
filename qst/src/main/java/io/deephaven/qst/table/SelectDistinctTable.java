//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class SelectDistinctTable extends TableBase implements SelectableTable {

    public static Builder builder() {
        return ImmutableSelectDistinctTable.builder();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends SelectableTable.Builder<SelectDistinctTable, SelectDistinctTable.Builder> {

    }
}
