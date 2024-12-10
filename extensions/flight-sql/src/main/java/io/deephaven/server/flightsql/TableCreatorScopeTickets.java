//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreatorDelegate;
import io.deephaven.qst.table.TicketTable;

import java.util.Map;
import java.util.Objects;

final class TableCreatorScopeTickets extends TableCreatorDelegate<Table> {

    static TicketTable ticketTable(String variableName) {
        return TicketTable.fromQueryScopeField(variableName);
    }

    private final Map<String, Table> map;

    TableCreatorScopeTickets(TableCreator<Table> delegate, Map<String, Table> map) {
        super(delegate);
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public Table of(TicketTable ticketTable) {
        final byte[] ticket = ticketTable.ticket();
        Assert.gt(ticket.length, "ticket.length", 2);
        Assert.eq(ticket[0], "ticket[0]", (byte) 's');
        Assert.eq(ticket[1], "ticket[1]", (byte) '/');
        final String variableName = new String(ticket, 2, ticket.length - 2);
        return Objects.requireNonNull(map.get(variableName));
    }
}
