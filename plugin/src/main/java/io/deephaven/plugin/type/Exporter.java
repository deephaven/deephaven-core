package io.deephaven.plugin.type;

import io.deephaven.proto.backplane.grpc.Ticket;

public interface Exporter {
    Reference newServerSideReference(Object object);

    interface Reference {
        Ticket id();
    }
}
