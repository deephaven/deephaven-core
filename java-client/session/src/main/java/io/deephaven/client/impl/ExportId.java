/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.proto.util.ExportTicketHelper;

import java.util.Optional;

/**
 * An opaque holder for a session export ID.
 */
public final class ExportId implements HasExportId {

    private final String type;
    private final int exportId;

    ExportId(String type, int exportId) {
        if (type != null && type.isEmpty()) {
            throw new IllegalArgumentException("Must use null instead of empty string to represent no type");
        }
        this.type = type;
        this.exportId = exportId;
    }

    public Optional<String> type() {
        return Optional.ofNullable(type);
    }

    @Override
    public ExportId exportId() {
        return this;
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(ticket());
    }

    @Override
    public TypedTicket typedTicket() {
        return new TypedTicket(type, this);
    }

    @Override
    public PathId pathId() {
        return new PathId(ExportTicketHelper.exportIdToPath(exportId));
    }

    @Override
    public String toString() {
        return (type == null ? "?:" : type + ":") + ExportTicketHelper.toReadableString(exportId);
    }

    int id() {
        return exportId;
    }

    byte[] ticket() {
        return ExportTicketHelper.exportIdToBytes(exportId);
    }

    ServerObject toServerObject(Session session) {
        if (type == null) {
            return new UnknownObject(session, this);
        }
        // noinspection SwitchStatementWithTooFewBranches
        switch (type) {
            case TableObject.TYPE:
                return new TableObject(session, this);
            default:
                return new CustomObject(session, this);
        }
    }
}
