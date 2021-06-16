package io.deephaven.qst.table;

import io.deephaven.qst.TableCreation;

/**
 * A "no-op" table creation impl, based on the QST structure itself. Mainly useful for testing the
 * equivalence for the {@link io.deephaven.qst.TableOperations} of {@link Table}; but publicly
 * available for functional completeness.
 */
public enum TableCreationImpl implements TableCreation<Table, Table> {
    INSTANCE;

    static Table toTable(Table table) {
        return TableCreation.create(INSTANCE, table);
    }

    @Override
    public final NewTable of(NewTable newTable) {
        return newTable;
    }

    @Override
    public final EmptyTable of(EmptyTable emptyTable) {
        return emptyTable;
    }

    @Override
    public final QueryScopeTable of(QueryScopeTable queryScopeTable) {
        return queryScopeTable;
    }
}
