//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleAdaptiveSparseArraySource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.DoubleSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.mutable.MutableLong;

import java.util.Random;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;

/**
 * The storages and row patterns compared by {@link SparseStorageBenchmark} and {@link SparseStorageMemory}.
 * <p>
 * Storages:
 * <ul>
 * <li>Sparse: {@link DoubleSparseArraySource}, a page table above full blocks</li>
 * <li>FlatHash: {@link DoubleHashColumnSource}, one hash table keyed by row key</li>
 * <li>Redirected: a {@link DoubleArraySource} of dense positions behind the hash {@link WritableRowRedirection} that
 * {@code QueryTable.redirectUpdate} uses</li>
 * <li>Adaptive: {@link DoubleAdaptiveSparseArraySource}, whose leaves are sorted, bitmap or full blocks by the number
 * of values they hold</li>
 * </ul>
 * Patterns cover a space of {@link #KEY_SPACE} row keys:
 * <ul>
 * <li>Dense: every row key</li>
 * <li>Stride8, Stride64, Stride512: every 8th, 64th or 512th row key</li>
 * <li>Runs64: the first 64 row keys of every 512</li>
 * <li>Random10, Random1: each row key with probability 10% or 1%</li>
 * <li>Clustered: an eighth of the leaves full, and 4 random row keys in each other leaf</li>
 * </ul>
 */
final class SparseStorage {
    static final long KEY_SPACE = 1L << 23;
    static final int CHUNK_SIZE = 4096;

    private SparseStorage() {}

    static RowSet keys(final String pattern) {
        final Random random = new Random(0);
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        switch (pattern) {
            case "Dense":
                return RowSetFactory.flat(KEY_SPACE);
            case "Stride8":
                return stride(8);
            case "Stride64":
                return stride(64);
            case "Stride512":
                return stride(512);
            case "Runs64":
                for (long key = 0; key < KEY_SPACE; key += 512) {
                    builder.appendRange(key, key + 63);
                }
                return builder.build();
            case "Random10":
                return random(random, 10);
            case "Random1":
                return random(random, 100);
            case "Clustered":
                for (long leafStart = 0; leafStart < KEY_SPACE; leafStart += BLOCK_SIZE) {
                    if (random.nextInt(8) == 0) {
                        builder.appendRange(leafStart, leafStart + BLOCK_SIZE - 1);
                    } else {
                        final long start = leafStart;
                        random.ints(0, BLOCK_SIZE).distinct().limit(4).sorted()
                                .forEach(offset -> builder.appendKey(start + offset));
                    }
                }
                return builder.build();
            default:
                throw new IllegalArgumentException("Unknown pattern " + pattern);
        }
    }

    private static RowSet stride(final int stride) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long key = 0; key < KEY_SPACE; key += stride) {
            builder.appendKey(key);
        }
        return builder.build();
    }

    private static RowSet random(final Random random, final int oneIn) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long key = 0; key < KEY_SPACE; ++key) {
            if (random.nextInt(oneIn) == 0) {
                builder.appendKey(key);
            }
        }
        return builder.build();
    }

    /**
     * Create an empty storage that will hold {@code expectedSize} values.
     */
    static WritableColumnSource<Double> create(final String storage, final int expectedSize) {
        switch (storage) {
            case "Sparse":
                return new DoubleSparseArraySource();
            case "Adaptive":
                return new DoubleAdaptiveSparseArraySource();
            case "FlatHash":
                return new DoubleHashColumnSource(expectedSize);
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
        return source;
    }
}
