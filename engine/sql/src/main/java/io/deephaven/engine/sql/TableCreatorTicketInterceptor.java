//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.sql;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.util.Map;
import java.util.Objects;

class TableCreatorTicketInterceptor implements TableCreator<Table> {
    private final TableCreator<Table> delegate;
    private final Map<TicketTable, Table> map;

    public TableCreatorTicketInterceptor(TableCreator<Table> delegate, Map<TicketTable, Table> map) {
        this.delegate = Objects.requireNonNull(delegate);
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public Table of(TicketTable ticketTable) {
        final Table table = map.get(ticketTable);
        if (table != null) {
            return table;
        }
        return delegate.of(ticketTable);
    }

    @Override
    public Table of(NewTable newTable) {
        return delegate.of(newTable);
    }

    @Override
    public Table of(EmptyTable emptyTable) {
        return delegate.of(emptyTable);
    }

    @Override
    public Table of(TimeTable timeTable) {
        return delegate.of(timeTable);
    }

    @Override
    public Table of(InputTable inputTable) {
        return delegate.of(inputTable);
    }

    @Override
    public Table merge(Iterable<Table> tables) {
        return delegate.merge(tables);
    }
}
