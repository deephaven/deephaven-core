package io.deephaven.client.examples;

import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.PathId;
import picocli.CommandLine.ArgGroup;

public class Path implements HasPathId {
    @ArgGroup(exclusive = false)
    ScopeField scopeField;

    @ArgGroup(exclusive = false)
    ApplicationField applicationField;

    private HasPathId get() {
        if (scopeField != null) {
            return scopeField;
        }
        if (applicationField != null) {
            return applicationField;
        }
        throw new IllegalStateException();
    }

    @Override
    public PathId pathId() {
        return get().pathId();
    }
}
