package io.deephaven.qst.table;

import io.deephaven.qst.column.Column;
import java.util.stream.Stream;

public abstract class NewTableBuildable {

    protected abstract Stream<Column<?>> columns();

    public final NewTable build() {
        return NewTable.of(() -> columns().iterator());
    }
}
