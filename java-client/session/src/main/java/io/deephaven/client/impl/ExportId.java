package io.deephaven.client.impl;

import io.deephaven.proto.util.ExportTicketHelper;

import java.util.Objects;

/**
 * An opaque holder for a session export ID.
 */
public final class ExportId implements HasExportId {

    private final String type;
    private final int exportId;

    ExportId(String type, int exportId) {
        this.type = Objects.requireNonNull(type);
        this.exportId = exportId;
    }

    public String type() {
        return type;
    }

    @Override
    public ExportId exportId() {
        return this;
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(ExportTicketHelper.exportIdToBytes(exportId));
    }

    @Override
    public PathId pathId() {
        return new PathId(ExportTicketHelper.exportIdToPath(exportId));
    }

    @Override
    public String toString() {
        return type + ":" + ExportTicketHelper.toReadableString(exportId);
    }

    int id() {
        return exportId;
    }
}
