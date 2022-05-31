package io.deephaven.client.examples;

import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.PathId;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.Option;

public class ScopeField implements HasTicketId, HasPathId {
    @Option(names = {"--variable"}, required = true, description = "The variable name.")
    String variable;

    public ScopeId scopeId() {
        return new ScopeId(variable);
    }

    @Override
    public TicketId ticketId() {
        return scopeId().ticketId();
    }

    @Override
    public PathId pathId() {
        return scopeId().pathId();
    }
}
