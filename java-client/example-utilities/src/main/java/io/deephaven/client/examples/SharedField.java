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
            description = "The shared identifier in hexadecimal format. ('0x' prefix is optional)")
    String sharedIdHex;

    public SharedId sharedId() {
        if (sharedIdHex.startsWith("0x")) {
            sharedIdHex = sharedIdHex.substring(2);
        }
        return new SharedId(ByteString.fromHex(sharedIdHex).toByteArray());
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
