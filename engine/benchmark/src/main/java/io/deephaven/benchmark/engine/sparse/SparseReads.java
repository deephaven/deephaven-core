//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.SafeCloseable;

import java.util.Random;

import static io.deephaven.benchmark.engine.sparse.SparseStorage.CHUNK_SIZE;

/**
 * The reads that {@link SparseStorageBenchmark} and {@link SparseSmallBenchmark} measure on a storage of
 * {@link SparseStorage}, with the contexts and chunks they need. Each read sums the values it reads.
 */
final class SparseReads implements SafeCloseable {
    private final WritableColumnSource<Double> source;
    /** Keys drawn at random from the storage's keys, in random order. */
    private final long[] randomKeys;
    private final WritableLongChunk<RowKeys> unorderedKeys;
    private final WritableDoubleChunk<Values> values;
    private final ChunkSource.FillContext fillContext;
    /** For the redirected storage, which does not fill unordered itself: its redirection and dense values. */
    private final RowRedirection redirection;
    private final ChunkSource.FillContext redirectionFillContext;
    private final FillUnordered<Values> denseValues;
    private final ChunkSource.FillContext denseFillContext;
    private final WritableLongChunk<RowKeys> positions;

    /**
     * @param source the storage to read
     * @param randomKeys keys of {@code source}, in the order that {@link #randomGet()} and {@link #fillUnordered()}
     *        read them
     */
    @SuppressWarnings("unchecked")
    SparseReads(final WritableColumnSource<Double> source, final long[] randomKeys) {
        this.source = source;
        this.randomKeys = randomKeys;
        unorderedKeys = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        values = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
        fillContext = source.makeFillContext(CHUNK_SIZE);
        if (source instanceof WritableRedirectedColumnSource) {
            final WritableRedirectedColumnSource<Double> redirected = (WritableRedirectedColumnSource<Double>) source;
            redirection = redirected.getRowRedirection();
            redirectionFillContext = redirection.makeFillContext(CHUNK_SIZE, null);
            final ColumnSource<Double> inner = redirected.getInnerSource();
            denseValues = (FillUnordered<Values>) inner;
            denseFillContext = inner.makeFillContext(CHUNK_SIZE);
            positions = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        } else {
            redirection = null;
            redirectionFillContext = null;
            denseValues = null;
            denseFillContext = null;
            positions = null;
        }
    }

    /**
     * @return {@code count} keys drawn at random from {@code keys} with {@code random}, in the order drawn
     */
    static long[] randomKeys(final RowSet keys, final int count, final Random random) {
        final long[] drawn = new long[count];
        for (int ii = 0; ii < count; ++ii) {
            drawn[ii] = keys.get(random.nextInt(keys.intSize()));
        }
        return drawn;
    }

    /**
     * Get the value of each random key, one at a time.
     */
    double randomGet() {
        double sum = 0;
        for (final long key : randomKeys) {
            sum += source.getDouble(key);
        }
        return sum;
    }

    /**
     * Fill chunks of the random keys, in random order. The redirected storage maps them to positions with its
     * redirection, then fills from its dense values.
     */
    double fillUnordered() {
        double sum = 0;
        for (int start = 0; start < randomKeys.length; start += CHUNK_SIZE) {
            final int size = Math.min(CHUNK_SIZE, randomKeys.length - start);
            unorderedKeys.copyFromTypedArray(randomKeys, start, 0, size);
            unorderedKeys.setSize(size);
            if (redirection != null) {
                redirection.fillChunkUnordered(redirectionFillContext, positions, unorderedKeys);
                denseValues.fillChunkUnordered(denseFillContext, values, positions);
            } else {
                ((FillUnordered<Values>) source).fillChunkUnordered(fillContext, values, unorderedKeys);
            }
            for (int ii = 0; ii < size; ++ii) {
                sum += values.get(ii);
            }
        }
        return sum;
    }

    /**
     * Fill chunks of {@code rows}, in key order.
     */
    double fill(final RowSet rows) {
        double sum = 0;
        try (final RowSequence.Iterator rowsIterator = rows.getRowSequenceIterator()) {
            while (rowsIterator.hasMore()) {
                source.fillChunk(fillContext, values, rowsIterator.getNextRowSequenceWithLength(CHUNK_SIZE));
                for (int ii = 0; ii < values.size(); ++ii) {
                    sum += values.get(ii);
                }
            }
        }
        return sum;
    }

    @Override
    public void close() {
        SafeCloseable.closeAll(positions, denseFillContext, redirectionFillContext, fillContext, values, unorderedKeys);
    }
}
