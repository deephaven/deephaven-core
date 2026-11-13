//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures populating and reading each storage of {@link SparseStorage} for each pattern of {@link SparsePatterns}.
 * Each benchmark reports milliseconds per invocation; divide by the row counts the trial prints to compare patterns.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(2)
public class SparseStorageBenchmark {
    private static final int RANDOM_READS = 1_000_000;

    @Param({"Sparse", "Redirected", "Adaptive", "AdaptiveFull"})
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
    private SparseReads reads;

    @Setup(Level.Trial)
    public void setupTrial() {
        executionContext = SparseStorage.openContext();

        keys = SparsePatterns.keys(pattern);
        source = SparseStorage.populate(storage, keys);

        final Random random = new Random(0);
        final RowSetBuilderSequential subsetBuilder = RowSetFactory.builderSequential();
        keys.forAllRowKeys(key -> {
            if (random.nextInt(10) == 0) {
                subsetBuilder.appendKey(key);
            }
        });
        randomSubset = subsetBuilder.build();
        randomKeys = SparseReads.randomKeys(keys, RANDOM_READS, random);
        reads = new SparseReads(source, randomKeys);
        System.out.println("\nstorage=" + storage + ", pattern=" + pattern + ", rows=" + keys.size()
                + ", subsetRows=" + randomSubset.size() + ", randomReads=" + RANDOM_READS);
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        SafeCloseable.closeAll(reads, randomSubset, keys, executionContext);
    }

    @Benchmark
    public WritableColumnSource<Double> populate() {
        return SparseStorage.populate(storage, keys);
    }

    @Benchmark
    public double sequentialFill() {
        return reads.fill(keys);
    }

    @Benchmark
    public double randomSubsetFill() {
        return reads.fill(randomSubset);
    }

    @Benchmark
    public double fillUnordered() {
        return reads.fillUnordered();
    }

    @Benchmark
    public double randomGet() {
        return reads.randomGet();
    }

    @Benchmark
    public WritableColumnSource<Double> randomSet() {
        double value = 0;
        for (final long key : randomKeys) {
            source.set(key, value);
            value += 1;
        }
        return source;
    }

    public static void main(final String[] args) {
        BenchUtil.run(16, SparseStorageBenchmark.class);
    }
}
