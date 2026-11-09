//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleAdaptiveSparseArraySource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.DoubleSparseArraySource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.Nullable;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.function.Supplier;

/**
 * The storages that the sparse benchmarks compare, holding the row keys of {@link SparsePatterns}, and how to measure
 * the heap they retain.
 * <p>
 * Storages:
 * <ul>
 * <li>Sparse: {@link DoubleSparseArraySource}, which holds every leaf as a full block</li>
 * <li>Redirected: a {@link DoubleArraySource} of dense positions behind the hash {@link WritableRowRedirection} that
 * {@code QueryTable.redirectUpdate} uses</li>
 * <li>Adaptive: {@link DoubleAdaptiveSparseArraySource} with the default thresholds, whose leaves are sorted, bitmap or
 * full blocks by the number of values they hold</li>
 * <li>AdaptiveFull: {@link DoubleAdaptiveSparseArraySource} with every leaf a full block, as the factory methods of
 * {@link SparseArrayColumnSource} create it</li>
 * </ul>
 */
final class SparseStorage {
    static final int CHUNK_SIZE = 4096;

    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();

    private SparseStorage() {}

    /**
     * Create an empty storage that will hold {@code expectedSize} values.
     */
    static WritableColumnSource<Double> create(final String storage, final int expectedSize) {
        switch (storage) {
            case "Sparse":
                return new DoubleSparseArraySource();
            case "Adaptive":
                return new DoubleAdaptiveSparseArraySource();
            case "AdaptiveFull":
                return DoubleAdaptiveSparseArraySource.fullBlocks();
            case "Redirected": {
                final DoubleArraySource dense = new DoubleArraySource();
                dense.ensureCapacity(expectedSize);
                return WritableRedirectedColumnSource.maybeRedirect(
                        WritableRowRedirection.FACTORY.createRowRedirection(expectedSize), dense, expectedSize);
            }
            default:
                throw new IllegalArgumentException("Unknown storage " + storage);
        }
    }

    /**
     * Create a storage and write a value for every key, a chunk at a time, as an initial select() or update() would.
     * The redirected storage first assigns each key the next dense position, as its redirection layer would.
     */
    static WritableColumnSource<Double> populate(final String storage, final RowSet keys) {
        final WritableColumnSource<Double> source = create(storage, keys.intSize());
        final WritableRowRedirection redirection = storage.equals("Redirected")
                ? (WritableRowRedirection) ((WritableRedirectedColumnSource<Double>) source).getRowRedirection()
                : null;
        fill(source, keys, redirection);
        return source;
    }

    /**
     * Write the position of each key of {@code keys}, from 0, as its value in {@code source}, a chunk at a time, first
     * mapping each key to its position in {@code redirection}, if not null.
     */
    static void fill(
            final WritableColumnSource<Double> source,
            final RowSet keys,
            @Nullable final WritableRowRedirection redirection) {
        long nextPosition = 0;
        try (final ChunkSink.FillFromContext context = source.makeFillFromContext(CHUNK_SIZE);
                final WritableDoubleChunk<Values> values = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
                final RowSequence.Iterator keysIterator = keys.getRowSequenceIterator()) {
            while (keysIterator.hasMore()) {
                final RowSequence chunkKeys = keysIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                final int size = chunkKeys.intSize();
                for (int ii = 0; ii < size; ++ii) {
                    values.set(ii, nextPosition + ii);
                }
                values.setSize(size);
                if (redirection != null) {
                    final MutableLong position = new MutableLong(nextPosition);
                    chunkKeys.forAllRowKeys(key -> redirection.put(key, position.getAndIncrement()));
                }
                source.fillFromChunk(context, values, chunkKeys);
                nextPosition += size;
            }
        }
    }

    /**
     * @return an open execution context whose update graph is in unit test mode, as the storages need outside an update
     *         cycle
     */
    static SafeCloseable openContext() {
        final SafeCloseable context = TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
        return context;
    }

    /**
     * @return the heap that the sources {@code populate} creates retain: the heap in use after creating them less the
     *         heap in use before, each once a full collection stops reducing it. Each source must hold 0 at the first
     *         key of {@code keys}.
     */
    static long retainedHeap(final Supplier<WritableColumnSource<Double>[]> populate, final RowSet keys)
            throws InterruptedException {
        final long before = usedAfterGc();
        final WritableColumnSource<Double>[] sources = populate.get();
        final long retained = usedAfterGc() - before;
        // read the sources after measuring, so that they are retained through the measurement
        for (final WritableColumnSource<Double> source : sources) {
            if (source.getDouble(keys.firstRowKey()) != 0) {
                throw new IllegalStateException("Unexpected first value");
            }
        }
        return retained;
    }

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
}
