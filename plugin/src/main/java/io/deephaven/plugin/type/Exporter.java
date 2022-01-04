package io.deephaven.plugin.type;

import io.deephaven.proto.backplane.grpc.Ticket;

public interface Exporter {
    Export newServerSideExport(Object export);

    interface Export {
        Ticket id();
    }
}
