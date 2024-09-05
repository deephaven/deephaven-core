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
    private final SessionState sessionState;

    TableCreatorScopeTickets(TableCreator<Table> delegate, ScopeTicketResolver scopeTicketResolver,
            SessionState sessionState) {
        super(delegate);
        this.scopeTicketResolver = Objects.requireNonNull(scopeTicketResolver);
        this.sessionState = sessionState;
    }

    @Override
    public Table of(TicketTable ticketTable) {
        // This does not wrap in a nugget like TicketRouter.resolve; is that important?
        return scopeTicketResolver.<Table>resolve(sessionState, ByteBuffer.wrap(ticketTable.ticket()),
                TableCreatorScopeTickets.class.getSimpleName()).get();
    }
}
