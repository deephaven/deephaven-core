//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmark.engine.PositionalSelectBenchmark.FORMULA;
import static io.deephaven.benchmark.engine.PositionalSelectBenchmark.createSource;
import static io.deephaven.benchmark.engine.PositionalSelectBenchmark.describe;
import static io.deephaven.benchmark.engine.PositionalSelectBenchmark.locked;

/**
 * Measures reads of the column produced by {@code update()} on a sparse, refreshing table, for each of the storage
 * modes described by {@link PositionalSelectBenchmark}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PositionalSelectReadBenchmark {
    private static final int CHUNK_SIZE = 4096;
    private static final int RANDOM_GETS = 1_000_000;

    @Param({"Positional", "Sparse", "Hash", "Flatten"})
    private String mode;

    @Param({"Stride8", "Runs64", "Stride64"})
    private String pattern;

    private SafeCloseable resultScope;
    private Table result;
    private ColumnSource<Double> xSource;
    private RowSet rows;
    /** A random tenth of the rows, in key order. */
    private RowSet randomSubset;
    /** Row keys in random order. */
    private long[] randomKeys;

    @Setup(Level.Trial)
    public void setupTrial() {
        final Table source = createSource(mode, pattern, 20_000_000L);
        resultScope = LivenessScopeStack.open();
        result = locked(() -> source.update(FORMULA));
        xSource = result.getColumnSource("X", double.class);
        rows = result.getRowSet();
        System.out.println("\nmode=" + mode + ", pattern=" + pattern + ", rows=" + rows.size() + ", X="
                + describe(xSource));

        final Random random = new Random(0);
        final RowSetBuilderSequential subsetBuilder = RowSetFactory.builderSequential();
        rows.forAllRowKeys(key -> {
            if (random.nextInt(10) == 0) {
                subsetBuilder.appendKey(key);
            }
        });
        randomSubset = subsetBuilder.build();
        randomKeys = new long[RANDOM_GETS];
        for (int ii = 0; ii < RANDOM_GETS; ++ii) {
            randomKeys[ii] = rows.get(random.nextInt(rows.intSize()));
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        randomSubset.close();
        resultScope.close();
    }

    @Benchmark
    public double sequentialFill() {
        return fillAndSum(rows);
    }

    @Benchmark
    public double randomSubsetFill() {
        return fillAndSum(randomSubset);
    }

    @Benchmark
    public double randomGet() {
        double sum = 0;
        for (final long key : randomKeys) {
            sum += xSource.getDouble(key);
        }
        return sum;
    }

    @Benchmark
    public long sumByKey() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return locked(() -> result.view("Key", "X").sumBy("Key")).size();
        }
    }

    private double fillAndSum(final RowSet rowsToRead) {
        double sum = 0;
        try (final ChunkSource.FillContext fillContext = xSource.makeFillContext(CHUNK_SIZE);
                final WritableDoubleChunk<Values> values = WritableDoubleChunk.makeWritableChunk(CHUNK_SIZE);
                final RowSequence.Iterator rowsIterator = rowsToRead.getRowSequenceIterator()) {
            while (rowsIterator.hasMore()) {
                final RowSequence chunkRows = rowsIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                xSource.fillChunk(fillContext, values, chunkRows);
                for (int ii = 0; ii < values.size(); ++ii) {
                    sum += values.get(ii);
                }
            }
        }
        return sum;
    }

    public static void main(final String[] args) {
        BenchUtil.run(16, PositionalSelectReadBenchmark.class);
    }
}
