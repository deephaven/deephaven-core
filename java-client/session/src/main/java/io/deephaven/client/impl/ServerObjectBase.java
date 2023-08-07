/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ObjectService.MessageStream;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class ServerObjectBase implements ServerObject {

    static void checkType(String expected, ExportId exportId) {
        final String actual = exportId.type().orElse(null);
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(String.format("Invalid type. expected=%s, actual=%s", expected, actual));
        }
    }

    final Session session;
    private final ExportId exportId;

    ServerObjectBase(Session session, ExportId exportId) {
        this.session = Objects.requireNonNull(session);
        this.exportId = Objects.requireNonNull(exportId);
    }

    public CompletableFuture<FetchedObject> fetch() {
        return session.fetchObject(this);
    }

    public MessageStream<HasTypedTicket> messageStream(MessageStream<ServerObject> stream) {
        return session.messageStream(this, stream);
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
    public CompletableFuture<Void> release() {
        return session.release(exportId);
    }

    @Override
    public final void close() {
        session.release(exportId);
    }

    @Override
    public String toString() {
        return exportId.toString();
    }
}
