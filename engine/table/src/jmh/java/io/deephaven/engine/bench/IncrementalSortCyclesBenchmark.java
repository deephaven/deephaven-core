package io.deephaven.engine.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * This benchmark is built around the basic construction:
 * 
 * <pre>
 * emptyTable(N)
 *         .select("Value=function(ii)")
 *         .where(incremental_release_filter)
 *         .sort("Value")
 * </pre>
 * 
 * Users are able to choose the function and control various sizing aspects for the table and
 * {@link io.deephaven.engine.table.impl.select.IncrementalReleaseFilter}.
 */
public class IncrementalSortCyclesBenchmark extends IncrementalSortCyclesBenchmarkBase {
    private static final int NUM_ROWS = 1000000;
    private static final String IDENTITY = "io.deephaven.engine.bench.Functions.identity";
    private static final String PRNG_100 = "io.deephaven.engine.bench.Functions.prng_100";
    private static final String PRNG_BINOMIAL_64 = "io.deephaven.engine.bench.Functions.prng_binomial_64";
    private static final String MOD_10 = "io.deephaven.engine.bench.Functions.mod_10";
    private static final String DIV_10 = "io.deephaven.engine.bench.Functions.div_10";

    // Note: I'm not sure if there is a better way to parameterize JMH. It could be nice if we could construct POJOs
    // instead to handoff to the JMH api; that would allow runtime parameterization for all of these fields.
    public enum Params {
        // @formatter:off
        IDENTITY_START_0_CYCLE_1k(0, 1000, false, IDENTITY),
        IDENTITY_START_0_CYCLE_10k(0, 10000, false, IDENTITY),
        IDENTITY_START_0_CYCLE_100k(0, 100000, false, IDENTITY),
        IDENTITY_START_0_CYCLE_1m(0, 1000000, false, IDENTITY),

        IDENTITY_START_10M_CYCLE_1k(10000000, 1000, false, IDENTITY),
        IDENTITY_START_10M_CYCLE_10k(10000000, 10000, false, IDENTITY),
        IDENTITY_START_10M_CYCLE_100k(10000000, 100000, false, IDENTITY),
        IDENTITY_START_10M_CYCLE_1m(10000000, 1000000, false, IDENTITY),

        REVERSE_START_0_CYCLE_1k(0, 1000, true, IDENTITY),
        REVERSE_START_0_CYCLE_10k(0, 10000, true, IDENTITY),
        REVERSE_START_0_CYCLE_100k(0, 100000, true, IDENTITY),
        REVERSE_START_0_CYCLE_1m(0, 1000000, true, IDENTITY),

        REVERSE_START_10M_CYCLE_1k(10000000, 1000, true, IDENTITY),
        REVERSE_START_10M_CYCLE_10k(10000000, 10000, true, IDENTITY),
        REVERSE_START_10M_CYCLE_100k(10000000, 100000, true, IDENTITY),
        REVERSE_START_10M_CYCLE_1m(10000000, 1000000, true, IDENTITY),

        PRNG_100_START_0_CYCLE_1k(0, 1000, false, PRNG_100),
        PRNG_100_START_0_CYCLE_10k(0, 10000, false, PRNG_100),
        PRNG_100_START_0_CYCLE_100k(0, 100000, false, PRNG_100),
        PRNG_100_START_0_CYCLE_1m(0, 1000000, false, PRNG_100),

        PRNG_100_START_10M_CYCLE_1k(10000000, 1000, false, PRNG_100),
        PRNG_100_START_10M_CYCLE_10k(10000000, 10000, false, PRNG_100),
        PRNG_100_START_10M_CYCLE_100k(10000000, 100000, false, PRNG_100),
        PRNG_100_START_10M_CYCLE_1m(10000000, 1000000, false, PRNG_100),

        PRNG_BINOMIAL_64_START_0_CYCLE_1k(0, 1000, false, PRNG_BINOMIAL_64),
        PRNG_BINOMIAL_64_START_0_CYCLE_10k(0, 10000, false, PRNG_BINOMIAL_64),
        PRNG_BINOMIAL_64_START_0_CYCLE_100k(0, 100000, false, PRNG_BINOMIAL_64),
        PRNG_BINOMIAL_64_START_0_CYCLE_1m(0, 1000000, false, PRNG_BINOMIAL_64),

        PRNG_BINOMIAL_64_START_10M_CYCLE_1k(10000000, 1000, false, PRNG_BINOMIAL_64),
        PRNG_BINOMIAL_64_START_10M_CYCLE_10k(10000000, 10000, false, PRNG_BINOMIAL_64),
        PRNG_BINOMIAL_64_START_10M_CYCLE_100k(10000000, 100000, false, PRNG_BINOMIAL_64),
        PRNG_BINOMIAL_64_START_10M_CYCLE_1m(10000000, 1000000, false, PRNG_BINOMIAL_64),

        MOD_10_START_0_CYCLE_1k(0, 1000, false, MOD_10),
        MOD_10_START_0_CYCLE_10k(0, 10000, false, MOD_10),
        MOD_10_START_0_CYCLE_100k(0, 100000, false, MOD_10),
        MOD_10_START_0_CYCLE_1m(0, 1000000, false, MOD_10),

        MOD_10_START_10M_CYCLE_1k(10000000, 1000, false, MOD_10),
        MOD_10_START_10M_CYCLE_10k(10000000, 10000, false, MOD_10),
        MOD_10_START_10M_CYCLE_100k(10000000, 100000, false, MOD_10),
        MOD_10_START_10M_CYCLE_1m(10000000, 1000000, false, MOD_10),

        DIV_10_START_0_CYCLE_1k(0, 1000, false, DIV_10),
        DIV_10_START_0_CYCLE_10k(0, 10000, false, DIV_10),
        DIV_10_START_0_CYCLE_100k(0, 100000, false, DIV_10),
        DIV_10_START_0_CYCLE_1m(0, 1000000, false, DIV_10),

        DIV_10_START_10M_CYCLE_1k(10000000, 1000, false, DIV_10),
        DIV_10_START_10M_CYCLE_10k(10000000, 10000, false, DIV_10),
        DIV_10_START_10M_CYCLE_100k(10000000, 100000, false, DIV_10),
        DIV_10_START_10M_CYCLE_1m(10000000, 1000000, false, DIV_10),
        ;
        // @formatter:on


        private final long initialSize;
        private final long cycleSize;
        private final int numCycles;
        private final boolean reversed;
        private final String indexToValueFunction;

        Params(int initialSize, int cycleSize, boolean reversed, String indexToValueFunction) {
            if (NUM_ROWS % cycleSize != 0) {
                throw new IllegalArgumentException(String.format("Cycle size must divide evenly into %d", NUM_ROWS));
            }
            this.initialSize = initialSize;
            this.cycleSize = cycleSize;
            this.numCycles = NUM_ROWS / cycleSize;
            this.reversed = reversed;
            this.indexToValueFunction = indexToValueFunction;
        }
    }

    @Param
    public Params params;

    @Setup(Level.Invocation)
    public void setup(Blackhole blackhole) throws Exception {
        init(
                params.initialSize,
                params.cycleSize,
                params.numCycles,
                params.indexToValueFunction,
                params.reversed,
                blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_ROWS)
    public void numRows() throws Throwable {
        runCycles();
    }
}
