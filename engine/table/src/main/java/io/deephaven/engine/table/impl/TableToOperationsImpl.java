//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator.TableToOperations;

enum TableToOperationsImpl implements TableToOperations<Table, Table> {
    INSTANCE;

    @Override
    public final Table of(Table table) {
        return table;
    }
}
