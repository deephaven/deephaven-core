package io.deephaven.qst.column;

public interface ColumnBuilder<T> {

    ColumnBuilder<T> add(T item);

    Column<T> build();
}
