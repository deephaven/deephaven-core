//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * Prints the heap that each storage of {@link SparseStorage} retains after populating each row pattern, as bytes per
 * row. "Redirected2" is two redirected columns that share one redirection, reported per column. Arguments optionally
 * restrict the storages, then the patterns, as comma-separated lists.
 */
public final class SparseStorageMemory {
    private static final String[] STORAGES = {"Sparse", "FlatHash", "Redirected", "Redirected2", "Adaptive"};
    private static final String[] PATTERNS = {"Dense", "Stride8", "Stride64", "Stride512", "Runs64", "Random10",
            "Random1", "Clustered"};
    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();

    private SparseStorageMemory() {}

    /**
     * @return the heap in use once a full collection stops reducing it
     */
    private static long usedAfterGc() throws InterruptedException {
        long last = Long.MAX_VALUE;
        for (int attempt = 0; attempt < 20; ++attempt) {
            System.gc();
            Thread.sleep(100);
            final long used = MEMORY.getHeapMemoryUsage().getUsed();
            if (Math.abs(last - used) < 64 * 1024) {
                return used;
            }
            last = used;
        }
        return last;
    }

    @SuppressWarnings("unchecked")
    private static WritableColumnSource<Double>[] single(final WritableColumnSource<Double> source) {
        return new WritableColumnSource[] {source};
    }

    /**
     * Populate two redirected columns that share one redirection, as the columns of one redirected update() do.
     */
    @SuppressWarnings("unchecked")
    private static WritableColumnSource<Double>[] populateShared(final RowSet keys) {
        final WritableRedirectedColumnSource<Double> first =
                (WritableRedirectedColumnSource<Double>) SparseStorage.populate("Redirected", keys);
        final DoubleArraySource secondDense = new DoubleArraySource();
        secondDense.ensureCapacity(keys.size());
        final WritableColumnSource<Double> second =
                WritableRedirectedColumnSource.maybeRedirect(first.getRowRedirection(), secondDense, keys.size());
        try (final ChunkSink.FillFromContext context = second.makeFillFromContext(SparseStorage.CHUNK_SIZE);
                final WritableDoubleChunk<Values> values =
                        WritableDoubleChunk.makeWritableChunk(SparseStorage.CHUNK_SIZE);
                final RowSequence.Iterator keysIterator = keys.getRowSequenceIterator()) {
            long nextPosition = 0;
            while (keysIterator.hasMore()) {
                final RowSequence chunkKeys = keysIterator.getNextRowSequenceWithLength(SparseStorage.CHUNK_SIZE);
                final int size = chunkKeys.intSize();
                for (int ii = 0; ii < size; ++ii) {
                    values.set(ii, nextPosition + ii);
                }
                values.setSize(size);
                second.fillFromChunk(context, values, chunkKeys);
                nextPosition += size;
            }
        }
        return new WritableColumnSource[] {first, second};
    }

    public static void main(final String[] args) throws InterruptedException {
        final String[] storages = args.length > 0 ? args[0].split(",") : STORAGES;
        final String[] patterns = args.length > 1 ? args[1].split(",") : PATTERNS;
        try (final SafeCloseable ignored = TestExecutionContext.createForUnitTests().open()) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
            System.out.printf("%-16s %-10s %10s %12s %10s%n", "storage", "pattern", "rows", "bytes", "bytes/row");
            for (final String pattern : patterns) {
                try (final RowSet keys = SparseStorage.keys(pattern)) {
                    for (final String storage : storages) {
                        final long before = usedAfterGc();
                        final WritableColumnSource<Double>[] sources = storage.equals("Redirected2")
                                ? populateShared(keys)
                                : single(SparseStorage.populate(storage, keys));
                        final long retained = usedAfterGc() - before;
                        System.out.printf("%-16s %-10s %10d %12d %10.2f%n", storage, pattern, keys.size(), retained,
                                (double) retained / ((long) sources.length * keys.size()));
                        // read the sources after measuring, so that they are retained through the measurement
                        for (final WritableColumnSource<Double> source : sources) {
                            if (source.getDouble(keys.firstRowKey()) != 0) {
                                throw new IllegalStateException("Unexpected first value");
                            }
                        }
                    }
                }
            }
        }
    }
}
