package io.deephaven.qst.table;

import io.deephaven.qst.TableToOperations;

enum TableToOperationsImpl implements TableToOperations<Table, Table> {
    INSTANCE;

    @Override
    public final Table of(Table table) {
        return table;
    }
}
