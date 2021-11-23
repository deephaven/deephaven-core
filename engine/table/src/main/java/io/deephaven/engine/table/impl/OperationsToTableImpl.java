package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator.OperationsToTable;

enum OperationsToTableImpl implements OperationsToTable<Table, Table> {
    INSTANCE;

    @Override
    public final Table of(Table table) {
        return table;
    }
}
