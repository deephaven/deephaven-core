//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.proto.util.SharedTicketHelper;

import java.util.Objects;

/**
 * An opaque holder for a shared object ID.
 */
public class SharedId implements HasTicketId, HasPathId {

    private final ByteString sharedId;

    public SharedId(final ByteString sharedId) {
        this.sharedId = Objects.requireNonNull(sharedId);
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(SharedTicketHelper.nameToBytes(sharedId));
    }

    @Override
    public PathId pathId() {
        return new PathId(SharedTicketHelper.nameToPath(sharedId));
    }
}
