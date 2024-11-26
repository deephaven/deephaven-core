//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.MultiJoinInput;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.util.List;
import java.util.Objects;

public abstract class TableCreatorDelegate<TABLE> implements TableCreator<TABLE> {
    private final TableCreator<TABLE> delegate;

    public TableCreatorDelegate(TableCreator<TABLE> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public TABLE of(NewTable newTable) {
        return delegate.of(newTable);
    }

    @Override
    public TABLE of(EmptyTable emptyTable) {
        return delegate.of(emptyTable);
    }

    @Override
    public TABLE of(TimeTable timeTable) {
        return delegate.of(timeTable);
    }

    @Override
    public TABLE of(TicketTable ticketTable) {
        return delegate.of(ticketTable);
    }

    @Override
    public TABLE of(InputTable inputTable) {
        return delegate.of(inputTable);
    }

    @Override
    public TABLE multiJoin(List<MultiJoinInput<TABLE>> multiJoinInputs) {
        return delegate.multiJoin(multiJoinInputs);
    }

    @Override
    public TABLE merge(Iterable<TABLE> tables) {
        return delegate.merge(tables);
    }
}
