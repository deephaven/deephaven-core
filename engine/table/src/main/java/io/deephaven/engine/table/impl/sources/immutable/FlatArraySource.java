package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;

public interface FlatArraySource<T> extends ColumnSource<T> {
    Object getArray();

    void setArray(Object array);
}
