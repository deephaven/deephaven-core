package io.deephaven.db.tables;

import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.qst.TableCreation;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.QueryScopeTable;

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
        return TableTools.emptyTable(emptyTable.size());
    }

    @Override
    public final Table of(QueryScopeTable queryScopeTable) {
        // todo: use DI and/or context instead of implicit scope?

        final Table table = QueryScope.getScope().readParamValue(queryScopeTable.variableName());
        // TODO: check header before returning
        return table;
    }
}
