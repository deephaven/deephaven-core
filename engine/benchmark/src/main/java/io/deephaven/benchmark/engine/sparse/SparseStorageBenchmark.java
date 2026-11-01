//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmark.engine.sparse.SparseStorage.CHUNK_SIZE;

/**
 * Measures populating and reading each storage of {@link SparseStorage} for each of its row patterns. Each benchmark
 * reports milliseconds per invocation; divide by the row counts the trial prints to compare patterns.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(2)
public class SparseStorageBenchmark {
    private static final int RANDOM_READS = 1_000_000;

    @Param({"Sparse", "Redirected", "Adaptive"})
    private String storage;

    @Param({"Dense", "Stride8", "Stride64", "Stride512", "Runs64", "Random10", "Random1", "Clustered"})
    private String pattern;

    private SafeCloseable executionContext;
    private RowSet keys;
    private WritableColumnSource<Double> source;
    /** A random tenth of the keys, in key order. */
    private RowSet randomSubset;
    /** Keys drawn at random from {@link #keys}, in random order. */
    private long[] randomKeys;
    private WritableLongChunk<RowKeys> unorderedKeys;
    private WritableDoubleChunk<Values> values;
    private ChunkSource.FillContext fillContext;
    /** For the redirected storage, which does not fill unordered itself: its redirection and dense values. */
    private RowRedirection redirection;
    private ChunkSource.FillContext redirectionFillContext;
    private FillUnordered<Values> denseValues;
    private ChunkSource.FillContext denseFillContext;
    private WritableLongChunk<RowKeys> positions;

    @Setup(Level.Trial)
    public void setupTrial() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();

        keys = SparseStorage.keys(pattern);
        source = SparseStorage.populate(storage, keys);

        final Random random = new Random(0);
        final RowSetBuilderSequential subsetBuilder = RowSetFactory.builderSequential();
        keys.forAllRowKeys(key -> {
            if (random.nextInt(10) == 0) {
                subsetBuilder.appendKey(key);
            }
        });
        randomSubset = subsetBuilder.build();
        randomKeys = new long[RANDOM_READS];
        for (int ii = 0; ii < RANDOM_READS; ++ii) {
            randomKeys[ii] = keys.get(random.nextInt(keys.intSize()));
        }
        unorderedKeys = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        values = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
        fillContext = source.makeFillContext(CHUNK_SIZE);
        if (storage.equals("Redirected")) {
            final WritableRedirectedColumnSource<Double> redirected = (WritableRedirectedColumnSource<Double>) source;
            redirection = redirected.getRowRedirection();
            redirectionFillContext = redirection.makeFillContext(CHUNK_SIZE, null);
            final ColumnSource<Double> inner = redirected.getInnerSource();
            denseValues = (FillUnordered<Values>) inner;
            denseFillContext = inner.makeFillContext(CHUNK_SIZE);
            positions = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        }
        System.out.println("\nstorage=" + storage + ", pattern=" + pattern + ", rows=" + keys.size()
                + ", subsetRows=" + randomSubset.size() + ", randomReads=" + RANDOM_READS);
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        SafeCloseable.closeAll(positions, denseFillContext, redirectionFillContext, fillContext, values, unorderedKeys,
                randomSubset, keys, executionContext);
    }

    @Benchmark
    public WritableColumnSource<Double> populate() {
        return SparseStorage.populate(storage, keys);
    }

    @Benchmark
    public double sequentialFill() {
        return fillAndSum(keys);
    }

    @Benchmark
    public double randomSubsetFill() {
        return fillAndSum(randomSubset);
    }

    /**
     * Fill chunks of keys in random order. The redirected storage maps them to positions with its redirection, then
     * fills from its dense values.
     */
    @Benchmark
    public double fillUnordered() {
        double sum = 0;
        for (int start = 0; start < RANDOM_READS; start += CHUNK_SIZE) {
            final int size = Math.min(CHUNK_SIZE, RANDOM_READS - start);
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

    @Benchmark
    public double randomGet() {
        double sum = 0;
        for (final long key : randomKeys) {
            sum += source.getDouble(key);
        }
        return sum;
    }

    private double fillAndSum(final RowSet rows) {
        double sum = 0;
        try (final RowSequence.Iterator rowsIterator = rows.getRowSequenceIterator()) {
            while (rowsIterator.hasMore()) {
                final RowSequence chunkRows = rowsIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                source.fillChunk(fillContext, values, chunkRows);
                for (int ii = 0; ii < values.size(); ++ii) {
                    sum += values.get(ii);
                }
            }
        }
        return sum;
    }

    public static void main(final String[] args) {
        BenchUtil.run(16, SparseStorageBenchmark.class);
    }
}
