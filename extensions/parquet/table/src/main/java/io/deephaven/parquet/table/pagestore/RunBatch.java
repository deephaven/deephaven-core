//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Collects runs to copy from one array to a chunk, and copies each batch of them with one {@link RunCopyKernel} call.
 * Short runs are common, and a {@link WritableChunk#copyFromArray} call per run is a megamorphic call per run.
 */
final class RunBatch implements SafeCloseable {
    /** Room for 1024 runs. */
    private static final int CAPACITY = 3 * 1024;

    private final WritableIntChunk<Any> runs = WritableIntChunk.makeWritableChunk(CAPACITY);
    private RunCopyKernel kernel;
    private Object source;
    private WritableChunk<?> destination;

    RunBatch() {
        runs.setSize(0);
    }

    /**
     * @param source The array to copy from, or {@code null} if no runs will be added
     */
    void begin(
            @NotNull final RunCopyKernel kernel,
            @Nullable final Object source,
            @NotNull final WritableChunk<?> destination) {
        this.kernel = kernel;
        this.source = source;
        this.destination = destination;
        runs.setSize(0);
    }

    void add(final int sourcePos, final int destinationPos, final int length) {
        if (runs.size() == CAPACITY) {
            flush();
        }
        runs.add(sourcePos);
        runs.add(destinationPos);
        runs.add(length);
    }

    /**
     * Copy the remaining runs and release the arrays.
     */
    void finish() {
        flush();
        source = null;
        destination = null;
    }

    private void flush() {
        if (runs.size() > 0) {
            // noinspection DataFlowIssue: runs are added only with a source
            kernel.copyRuns(source, runs, destination);
            runs.setSize(0);
        }
    }

    @Override
    public void close() {
        runs.close();
    }
}
