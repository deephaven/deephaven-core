package io.deephaven.qst.table.column;

public interface ColumnBuilder<T> {

  ColumnBuilder<T> add(T item);

  Column<T> build();
}
