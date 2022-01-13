package io.deephaven.server.object;

import io.deephaven.plugin.type.ObjectTypeLookup;

import java.util.Objects;

public class Type {

    private final ObjectTypeLookup lookup;

    public Type(ObjectTypeLookup lookup) {
        this.lookup = Objects.requireNonNull(lookup);
    }
}
