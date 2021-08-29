package io.deephaven.engine.tables;

import io.deephaven.qst.TableCreator.TableToOperations;

enum TableToOperationsImpl implements TableToOperations<Table, Table> {
    INSTANCE;

    @Override
    public final Table of(Table table) {
        return table;
    }
}
