package io.deephaven.client.impl;

import io.deephaven.proto.util.ScopeTicketHelper;

import java.util.Objects;

/**
 * An opaque holder for a query scope variable ID.
 */
public final class ScopeId implements HasTicketId, HasPathId {

    private final String variableName;

    public ScopeId(String variableName) {
        this.variableName = Objects.requireNonNull(variableName);
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(ScopeTicketHelper.nameToBytes(variableName));
    }

    @Override
    public PathId pathId() {
        return new PathId(ScopeTicketHelper.nameToPath(variableName));
    }
}
