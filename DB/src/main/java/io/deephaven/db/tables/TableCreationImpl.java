package io.deephaven.db.tables;

import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.qst.TableCreation;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;

enum TableCreationImpl implements TableCreation<Table, Table> {
    INSTANCE;

    public static Table create(io.deephaven.qst.table.Table table) {
        return TableCreation.create(INSTANCE, table).toTable();
    }

    @Override
    public final Table of(NewTable newTable) {
        return InMemoryTable.from(newTable);
    }

    @Override
    public final Table of(EmptyTable emptyTable) {
        return TableTools.emptyTable(emptyTable.size(), TableDefinition.from(emptyTable.header()));
    }
}
