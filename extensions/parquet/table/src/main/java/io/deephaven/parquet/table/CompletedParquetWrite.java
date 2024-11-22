//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

import java.net.URI;

/**
 * This class is used as a return POJO for the result of a Parquet write operation.
 * <p>
 * It is intended to be used with the {@link ParquetInstructions#onWriteCompleted()} callback.
 */
@Value.Immutable
@BuildableStyle
public abstract class CompletedParquetWrite {
    /**
     * The destination URI of the written Parquet file.
     */
    public abstract URI destination();

    /**
     * The number of rows written to the Parquet file.
     */
    public abstract long numRows();

    /**
     * The number of bytes written to the Parquet file.
     */
    public abstract long numBytes();

    public static Builder builder() {
        return ImmutableCompletedParquetWrite.builder();
    }

    interface Builder {
        Builder destination(URI destination);

        Builder numRows(long numRows);

        Builder numBytes(long numBytes);

        CompletedParquetWrite build();
    }

    @Value.Check
    final void numRowsBoundsCheck() {
        if (numRows() < 0) {
            throw new IllegalArgumentException("numRows must be non-negative");
        }
    }

    @Value.Check
    final void numBytesBoundsCheck() {
        if (numBytes() <= 0) {
            throw new IllegalArgumentException("numBytes must be positive");
        }
    }
}
