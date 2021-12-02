package io.deephaven.client.impl;

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

    public int createExportId() {
        return nextId.getAndIncrement();
    }
}
