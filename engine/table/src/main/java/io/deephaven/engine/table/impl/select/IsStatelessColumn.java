package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;

public class IsStatelessColumn {
    public static boolean isStateless(ColumnSource<?> columnSource) {
        return columnSource.isImmutable() || columnSource instanceof InMemoryColumnSource;
    }
}
