//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures populating and reading small results in each storage of {@link SparseStorage}, in the shapes of
 * {@link SparsePatterns#smallKeys(String, int)}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SparseSmallBenchmark {
    private static final int RANDOM_READS = 100_000;

    @Param({"Sparse", "Redirected", "Adaptive", "AdaptiveFull"})
    private String storage;

    @Param({"Dense", "Stride64", "High", "Spread", "Level2s", "Regions", "FarRegions"})
    private String shape;

    @Param({"3000", "30000"})
    private int size;

    private SafeCloseable executionContext;
    private RowSet keys;
    private SparseReads reads;

    @Setup(Level.Trial)
    public void setupTrial() {
        executionContext = SparseStorage.openContext();
        keys = SparsePatterns.smallKeys(shape, size);
        final WritableColumnSource<Double> source = SparseStorage.populate(storage, keys);
        reads = new SparseReads(source, SparseReads.randomKeys(keys, RANDOM_READS, new Random(0)));
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        SafeCloseable.closeAll(reads, keys, executionContext);
    }

    @Benchmark
    public WritableColumnSource<Double> populate() {
        return SparseStorage.populate(storage, keys);
    }

    @Benchmark
    public double randomGet() {
        return reads.randomGet();
    }

    /**
     * Fill chunks of every key, in key order.
     */
    @Benchmark
    public double sequentialFill() {
        return reads.fill(keys);
    }

    @Benchmark
    public double fillUnordered() {
        return reads.fillUnordered();
    }
}
