package io.deephaven.plugin.type;

import io.deephaven.proto.backplane.grpc.Ticket;

public interface Exporter {
    <T> Export<T> newServerSideExport(T export);

    interface Export<T> {
        T get();

        Ticket getExportId();
    }
}
