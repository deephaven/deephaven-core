//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.SharedTicketHelper;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * An opaque holder for a shared object ID.
 */
public class SharedId implements HasTicketId, HasPathId {

    /**
     * @return a new random {@link SharedId}
     */
    public static SharedId newRandom() {
        final ByteBuffer sharedId = ByteBuffer.allocate(16);
        final UUID uuid = UUID.randomUUID();
        sharedId.putLong(uuid.getMostSignificantBits());
        sharedId.putLong(uuid.getLeastSignificantBits());
        return new SharedId(sharedId.array());
    }

    private final byte[] sharedId;

    public SharedId(final byte[] sharedId) {
        this.sharedId = Objects.requireNonNull(sharedId);
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(SharedTicketHelper.idToBytes(sharedId));
    }

    @Override
    public PathId pathId() {
        return new PathId(SharedTicketHelper.idToPath(sharedId));
    }

    public String asHexString() {
        return "0x" + ByteHelper.byteBufToHex(ByteBuffer.wrap(sharedId));
    }
}
