//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.iceberg.util.IcebergReadInstructions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Used to hold a {@link Schema}, {@link PartitionSpec} and {@link IcebergReadInstructions} together.
 */
public final class SpecAndSchema {
    public final Schema schema;
    public final PartitionSpec partitionSpec;

    public SpecAndSchema(
            @NotNull final Schema schema,
            @NotNull final PartitionSpec partitionSpec) {
        this.schema = schema;
        this.partitionSpec = partitionSpec;
    }
}
