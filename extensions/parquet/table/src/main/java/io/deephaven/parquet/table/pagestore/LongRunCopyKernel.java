//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRunCopyKernel and run "./gradlew replicateRunCopyKernel" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

final class LongRunCopyKernel implements RunCopyKernel {
    static final LongRunCopyKernel INSTANCE = new LongRunCopyKernel();

    private LongRunCopyKernel() {}

    @Override
    public void copyRuns(
            @NotNull final Object source,
            @NotNull final IntChunk<? extends Any> runs,
            @NotNull final WritableChunk<?> destination) {
        final long[] typedSource = (long[]) source;
        final WritableLongChunk<?> typedDestination = destination.asWritableLongChunk();
        for (int ii = 0; ii < runs.size(); ii += 3) {
            typedDestination.copyFromTypedArray(typedSource, runs.get(ii), runs.get(ii + 1), runs.get(ii + 2));
        }
    }
}
