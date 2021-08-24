/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;

public class TableGetter {
    private final Table table;

    TableGetter(Table table) {
        this.table = table;
    }

    Object get(String columnName, int index) {
        return table.getColumn(columnName).get(index);
    }

    byte getByte(String columnName, int index) {
        return table.getColumn(columnName).getByte(index);
    }

    char getChar(String columnName, int index) {
        return table.getColumn(columnName).getChar(index);
    }

    double getDouble(String columnName, int index) {
        return table.getColumn(columnName).getDouble(index);
    }

    float getFloat(String columnName, int index) {
        return table.getColumn(columnName).getFloat(index);
    }

    int getInt(String columnName, int index) {
        return table.getColumn(columnName).getInt(index);
    }

    long getLong(String columnName, int index) {
        return table.getColumn(columnName).getLong(index);
    }

    short getShort(String columnName, int index) {
        return table.getColumn(columnName).getShort(index);
    }

}
