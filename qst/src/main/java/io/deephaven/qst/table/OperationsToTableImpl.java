package io.deephaven.qst.table;

import io.deephaven.qst.TableCreation.OperationsToTable;

enum OperationsToTableImpl implements OperationsToTable<TableSpec, TableSpec> {
    INSTANCE;

    @Override
    public final TableSpec of(TableSpec table) {
        return table;
    }
}
