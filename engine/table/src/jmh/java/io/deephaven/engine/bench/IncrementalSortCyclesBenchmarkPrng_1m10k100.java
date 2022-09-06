package io.deephaven.engine.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

public class IncrementalSortCyclesBenchmarkPrng_1m10k100 extends IncrementalSortCyclesBenchmarkBase {

    private static final int TABLE_SIZE = 1000000;
    private static final int CYCLE_SIZE = 10000;
    private static final int NUM_CYCLES = 100;
    private static final int NUM_ROWS = CYCLE_SIZE * NUM_CYCLES;

    @Setup(Level.Invocation)
    public void setup(Blackhole blackhole) throws Exception {
        init(TABLE_SIZE, CYCLE_SIZE, NUM_CYCLES, "io.deephaven.engine.bench.Functions.prng", false, blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_ROWS)
    public void numRows() throws Throwable {
        runCycles();
    }
}
