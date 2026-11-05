//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
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

/**
 * Measures populating and reading small results in each storage of {@link SparseStorage}, in the shapes of
 * {@link SparseSmallMemory}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SparseSmallBenchmark {
    private static final int RANDOM_READS = 100_000;

    @Param({"Sparse", "Redirected", "Adaptive"})
    private String storage;

    @Param({"Dense", "Stride64", "High", "Spread", "Level2s", "Regions", "FarRegions"})
    private String shape;

    @Param({"3000", "30000"})
    private int size;

    private SafeCloseable executionContext;
    private RowSet keys;
    private WritableColumnSource<Double> source;
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
        keys = SparseSmallMemory.keys(shape, size);
        source = SparseStorage.populate(storage, keys);
        final Random random = new Random(0);
        randomKeys = new long[RANDOM_READS];
        for (int ii = 0; ii < RANDOM_READS; ++ii) {
            randomKeys[ii] = keys.get(random.nextInt(keys.intSize()));
        }
        unorderedKeys = WritableLongChunk.makeWritableChunk(SparseStorage.CHUNK_SIZE);
        values = WritableDoubleChunk.makeWritableChunk(SparseStorage.CHUNK_SIZE);
        fillContext = source.makeFillContext(SparseStorage.CHUNK_SIZE);
        if (storage.equals("Redirected")) {
            final WritableRedirectedColumnSource<Double> redirected = (WritableRedirectedColumnSource<Double>) source;
            redirection = redirected.getRowRedirection();
            redirectionFillContext = redirection.makeFillContext(SparseStorage.CHUNK_SIZE, null);
            final ColumnSource<Double> inner = redirected.getInnerSource();
            denseValues = (FillUnordered<Values>) inner;
            denseFillContext = inner.makeFillContext(SparseStorage.CHUNK_SIZE);
            positions = WritableLongChunk.makeWritableChunk(SparseStorage.CHUNK_SIZE);
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        SafeCloseable.closeAll(positions, denseFillContext, redirectionFillContext, fillContext, values, unorderedKeys,
                keys, executionContext);
    }

    @Benchmark
    public WritableColumnSource<Double> populate() {
        return SparseStorage.populate(storage, keys);
    }

    @Benchmark
    public double randomGet() {
        double sum = 0;
        for (final long key : randomKeys) {
            sum += source.getDouble(key);
        }
        return sum;
    }

    /**
     * Fill chunks of every key, in key order.
     */
    @Benchmark
    public double sequentialFill() {
        double sum = 0;
        try (final RowSequence.Iterator keysIterator = keys.getRowSequenceIterator()) {
            while (keysIterator.hasMore()) {
                source.fillChunk(fillContext, values,
                        keysIterator.getNextRowSequenceWithLength(SparseStorage.CHUNK_SIZE));
                for (int ii = 0; ii < values.size(); ++ii) {
                    sum += values.get(ii);
                }
            }
        }
        return sum;
    }

    /**
     * Fill chunks of the random keys, in random order. The redirected storage maps them to positions with its
     * redirection, then fills from its dense values.
     */
    @Benchmark
    public double fillUnordered() {
        double sum = 0;
        for (int start = 0; start < RANDOM_READS; start += SparseStorage.CHUNK_SIZE) {
            final int size = Math.min(SparseStorage.CHUNK_SIZE, RANDOM_READS - start);
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
}
