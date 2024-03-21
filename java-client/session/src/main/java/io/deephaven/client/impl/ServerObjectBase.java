//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

abstract class ServerObjectBase implements ServerObject {

    static void checkType(String expected, ExportId exportId) {
        final String actual = exportId.type().orElse(null);
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(String.format("Invalid type. expected=%s, actual=%s", expected, actual));
        }
    }

    final Session session;
    final ExportId exportId;

    ServerObjectBase(Session session, ExportId exportId) {
        this.session = Objects.requireNonNull(session);
        this.exportId = Objects.requireNonNull(exportId);
    }

    @Override
    public final ExportId exportId() {
        return exportId;
    }

    @Override
    public final PathId pathId() {
        return exportId.pathId();
    }

    @Override
    public final TicketId ticketId() {
        return exportId.ticketId();
    }

    @Override
    public final TypedTicket typedTicket() {
        return exportId.typedTicket();
    }

    @Override
    public final CompletableFuture<Void> release() {
        return session.release(exportId);
    }

    @Override
    public final void close() {
        session.release(exportId);
    }

    @Override
    public final String toString() {
        return exportId.toString();
    }
}
