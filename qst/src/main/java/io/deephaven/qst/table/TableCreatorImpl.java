//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;

import java.util.List;

/**
 * A "no-op" table creator impl, based on the QST structure itself. Mainly useful for testing the equivalence for the
 * {@link TableOperations} of {@link TableSpec}; but publicly available for functional completeness.
 */
public enum TableCreatorImpl implements TableCreator<TableSpec> {
    INSTANCE;

    static TableSpec toTable(TableSpec table) {
        return table.logic().create(INSTANCE);
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
    public final TableSpec of(TicketTable ticketTable) {
        return ticketTable;
    }

    @Override
    public final TableSpec of(InputTable inputTable) {
        return inputTable;
    }

    @Override
    public final MultiJoinTable multiJoin(List<MultiJoinInput<TableSpec>> multiJoinInputs) {
        return MultiJoinTable.builder().addAllInputs(multiJoinInputs).build();
    }

    @Override
    public final MergeTable merge(Iterable<TableSpec> tables) {
        return MergeTable.builder().addAllTables(tables).build();
    }
}
