//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Snapshot;

import java.util.Objects;
import java.util.Optional;


final class ResolverAndSnapshot {
    private final Resolver resolver;
    private final Snapshot snapshot;

    ResolverAndSnapshot(final Resolver di, final Snapshot snapshot) {
        this.resolver = Objects.requireNonNull(di);
        this.snapshot = snapshot;
    }

    public Resolver resolver() {
        return resolver;
    }

    public Optional<Snapshot> snapshot() {
        return Optional.ofNullable(snapshot);
    }
}
