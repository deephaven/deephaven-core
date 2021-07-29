package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreation;

import java.util.Collection;

/**
 * A "no-op" table creation impl, based on the QST structure itself. Mainly useful for testing the
 * equivalence for the {@link TableOperations} of {@link TableSpec}; but publicly available for
 * functional completeness.
 */
public enum TableCreationImpl implements TableCreation<TableSpec> {
    INSTANCE;

    static TableSpec toTable(TableSpec table) {
        return TableCreation.create(INSTANCE, TableToOperationsImpl.INSTANCE,
            OperationsToTableImpl.INSTANCE, table);
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
    public final TimeTable of(TimeTable timeTable) {
        return timeTable;
    }

    @Override
    public final MergeTable merge(Collection<TableSpec> tables) {
        return ImmutableMergeTable.builder().addAllTables(tables).build();
    }
}
