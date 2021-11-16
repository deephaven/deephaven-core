package io.deephaven.engine.tables;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator.OperationsToTable;

enum OperationsToTableImpl implements OperationsToTable<Table, Table> {
    INSTANCE;

    @Override
    public final Table of(Table table) {
        return table;
    }
}
