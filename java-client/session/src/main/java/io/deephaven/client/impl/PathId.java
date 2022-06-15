/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.util.List;
import java.util.Objects;

/**
 * An opaque holder that represents a flight descriptor path.
 */
public final class PathId implements HasPathId {

    private final List<String> path;

    public PathId(List<String> path) {
        this.path = Objects.requireNonNull(path);
    }

    @Override
    public PathId pathId() {
        return this;
    }

    List<String> path() {
        return path;
    }
}
