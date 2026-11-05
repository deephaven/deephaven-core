//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.WritableColumnSource;
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

    @Param({"Dense", "Stride64", "High", "Spread"})
    private String shape;

    @Param({"3000", "30000"})
    private int size;

    private SafeCloseable executionContext;
    private RowSet keys;
    private WritableColumnSource<Double> source;
    private long[] randomKeys;

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
    }

    @TearDown(Level.Trial)
    public void teardownTrial() {
        SafeCloseable.closeAll(keys, executionContext);
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
}
