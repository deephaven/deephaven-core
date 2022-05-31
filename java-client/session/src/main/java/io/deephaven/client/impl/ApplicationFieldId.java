package io.deephaven.client.impl;

import io.deephaven.proto.util.ApplicationTicketHelper;

import java.util.Objects;

/**
 * An opaque holder for an application field ID.
 */
public final class ApplicationFieldId implements HasTicketId, HasPathId {

    private final String applicationId;
    private final String fieldName;

    public ApplicationFieldId(String applicationId, String fieldName) {
        this.applicationId = Objects.requireNonNull(applicationId);
        this.fieldName = Objects.requireNonNull(fieldName);
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(ApplicationTicketHelper.applicationFieldToBytes(applicationId, fieldName));
    }

    @Override
    public PathId pathId() {
        return new PathId(ApplicationTicketHelper.applicationFieldToPath(applicationId, fieldName));
    }
}
