package io.deephaven.client.impl;

import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.Ticket;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

final class ExportTicketCreator {

    private final AtomicInteger nextId;

    public ExportTicketCreator() {
        this(new AtomicInteger(1));
    }

    public ExportTicketCreator(AtomicInteger nextId) {
        this.nextId = Objects.requireNonNull(nextId);
    }

    public Ticket create() {
        return ExportTicketHelper.wrapExportIdInTicket(nextId.getAndIncrement());
    }
}
