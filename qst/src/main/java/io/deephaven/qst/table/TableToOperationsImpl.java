package io.deephaven.qst.table;

import io.deephaven.qst.TableToOperations;

enum TableToOperationsImpl implements TableToOperations<TableSpec, TableSpec> {
    INSTANCE;

    @Override
    public final TableSpec of(TableSpec table) {
        return table;
    }
}
