//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

public class TableAssert extends AbstractAssert<TableAssert, Table> {
    public static TableAssert assertThat(Table table) {
        return new TableAssert(table);
    }

    public TableAssert(Table table) {
        super(table, TableAssert.class);
    }

    @Override
    public TableAssert isEqualTo(Object expected) {
        Assertions.assertThat(expected).isInstanceOf(Table.class);
        TstUtils.assertTableEquals((Table) expected, actual);
        return this;
    }

    @Override
    public TableAssert isNotEqualTo(Object other) {
        throw new UnsupportedOperationException();
    }

    public RowSetAssert rowSet() {
        return RowSetAssert.assertThat(actual.getRowSet());
    }

    public <T> ColumnSourceAssert<T> columnSource(String name) {
        return new ColumnSourceAssert<>(actual.getColumnSource(name));
    }

    public <T> ColumnSourceAssert<T> columnSource(String name, Class<T> clazz) {
        return new ColumnSourceAssert<>(actual.getColumnSource(name, clazz));
    }

    public <T> ColumnSourceAssert<T> columnSource(String name, Class<T> clazz, Class<?> componentType) {
        return new ColumnSourceAssert<>(actual.getColumnSource(name, clazz, componentType));
    }

    public <T> ColumnSourceValuesAssert<T> columnSourceValues(String name) {
        return this.<T>columnSource(name).values(actual.getRowSet());
    }

    public <T> ColumnSourceValuesAssert<T> columnSourceValues(String name, Class<T> clazz) {
        return columnSource(name, clazz).values(actual.getRowSet());
    }

    public <T> ColumnSourceValuesAssert<T> columnSourceValues(String name, Class<T> clazz, Class<?> componentType) {
        return columnSource(name, clazz, componentType).values(actual.getRowSet());
    }

    public void isEmpty() {
        Assertions.assertThat(actual.isEmpty()).isTrue();
    }

    public void isNotEmpty() {
        Assertions.assertThat(actual.isEmpty()).isFalse();
    }

    public void isRefreshing() {
        Assertions.assertThat(actual.isRefreshing()).isTrue();
    }

    public void isNotRefreshing() {
        Assertions.assertThat(actual.isRefreshing()).isFalse();
    }

    public void isFlat() {
        Assertions.assertThat(actual.isFlat()).isTrue();
    }

    public void isNotFlat() {
        Assertions.assertThat(actual.isFlat()).isFalse();
    }

    public void isFailed() {
        Assertions.assertThat(actual.isFailed()).isTrue();
    }

    public void isNotFailed() {
        Assertions.assertThat(actual.isFailed()).isFalse();
    }
}
