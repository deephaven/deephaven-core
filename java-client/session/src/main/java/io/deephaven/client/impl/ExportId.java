package io.deephaven.client.impl;

import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.TicketTable;

import java.util.Arrays;
import java.util.List;

/**
 * An opaque holder for a session export ID.
 */
public final class ExportId implements HasExportId {

    private final int exportId;

    ExportId(int exportId) {
        this.exportId = exportId;
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
        return ExportTicketHelper.toReadableString(exportId);
    }

    int id() {
        return exportId;
    }
}
