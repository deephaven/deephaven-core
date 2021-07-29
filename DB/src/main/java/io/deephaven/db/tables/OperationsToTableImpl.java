package io.deephaven.db.tables;

import io.deephaven.qst.TableCreation.OperationsToTable;

enum OperationsToTableImpl implements OperationsToTable<Table, Table> {
    INSTANCE;

    @Override
    public final Table of(Table table) {
        return table;
    }
}
