//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.PathId;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.Option;

public class SharedField implements HasTicketId, HasPathId {
    @Option(names = {"--shared-id"}, required = true, description = "The shared variable identifier.")
    String sharedId;

    public SharedId sharedId() {
        return new SharedId(sharedId);
    }

    @Override
    public TicketId ticketId() {
        return sharedId().ticketId();
    }

    @Override
    public PathId pathId() {
        return sharedId().pathId();
    }
}
