//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.PathId;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.Option;

public class SharedField implements HasTicketId, HasPathId {
    @Option(names = {"--shared-id-hex"}, required = true,
            description = "The shared variable identifier in hexadecimal format.")
    String sharedIdHex;

    public SharedId sharedId() {
        return new SharedId(ByteString.fromHex(sharedIdHex));
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
