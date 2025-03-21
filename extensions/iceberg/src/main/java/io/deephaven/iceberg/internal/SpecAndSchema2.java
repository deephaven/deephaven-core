//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.iceberg.util.Resolver;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;

import java.util.Objects;

/**
 * Used to hold a {@link Schema}, {@link PartitionSpec} and {@link IcebergReadInstructions} together.
 */
public final class SpecAndSchema2 {
    public final Resolver di;
    // public final PartitionSpec partitionSpec;
    public final Snapshot snapshot;

    public SpecAndSchema2(Resolver di, /* PartitionSpec partitionSpec, */ Snapshot snapshot) {
        this.di = Objects.requireNonNull(di);
        // this.partitionSpec = Objects.requireNonNull(partitionSpec);
        this.snapshot = snapshot;
    }
}
