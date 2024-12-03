//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreatorDelegate;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.session.SessionState;

import java.nio.ByteBuffer;
import java.util.Objects;

final class TableCreatorScopeTickets extends TableCreatorDelegate<Table> {

    private final ScopeTicketResolver scopeTicketResolver;
    private final SessionState session;

    TableCreatorScopeTickets(TableCreator<Table> delegate, ScopeTicketResolver scopeTicketResolver,
            SessionState session) {
        super(delegate);
        this.scopeTicketResolver = Objects.requireNonNull(scopeTicketResolver);
        this.session = session;
    }

    @Override
    public Table of(TicketTable ticketTable) {
        return scopeTicketResolver.<Table>resolve(session, ByteBuffer.wrap(ticketTable.ticket()),
                TableCreatorScopeTickets.class.getSimpleName()).get();
    }
}
