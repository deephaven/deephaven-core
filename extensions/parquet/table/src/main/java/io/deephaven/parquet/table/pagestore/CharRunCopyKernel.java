//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

final class CharRunCopyKernel implements RunCopyKernel {
    static final CharRunCopyKernel INSTANCE = new CharRunCopyKernel();

    private CharRunCopyKernel() {}

    @Override
    public void copyRuns(
            @NotNull final Object source,
            @NotNull final IntChunk<? extends Any> runs,
            @NotNull final WritableChunk<?> destination) {
        final char[] typedSource = (char[]) source;
        final WritableCharChunk<?> typedDestination = destination.asWritableCharChunk();
        for (int ii = 0; ii < runs.size(); ii += 3) {
            typedDestination.copyFromTypedArray(typedSource, runs.get(ii), runs.get(ii + 1), runs.get(ii + 2));
        }
    }
}
