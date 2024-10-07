//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.sql;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreatorDelegate;
import io.deephaven.qst.table.TicketTable;

import java.util.Map;
import java.util.Objects;

class TableCreatorTicketInterceptor extends TableCreatorDelegate<Table> {
    private final Map<TicketTable, Table> map;

    public TableCreatorTicketInterceptor(TableCreator<Table> delegate, Map<TicketTable, Table> map) {
        super(delegate);
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public Table of(TicketTable ticketTable) {
        final Table table = map.get(ticketTable);
        if (table != null) {
            return table;
        }
        return super.of(ticketTable);
    }
}
