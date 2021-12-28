package io.deephaven.benchmark.engine.util;


import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.engine.rowset.impl.RspBitmapBuilderSequential;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRangesInt;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRangesLong;
import gnu.trove.set.hash.TIntHashSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(timeUnit = TimeUnit.MILLISECONDS, iterations = 2, time = 2000)
@Measurement(timeUnit = TimeUnit.MILLISECONDS, iterations = 5, time = 2000)
@Fork(1)
public class SmallRowSetCreation {
    // @Param({"12", "16", "20"})
    @Param("12")
    private int valuesPerBlock;

    // @Param({"7000", "8000", "9000"})
    @Param("8000")
    private int cardinality;

    long[] values;

    private static final long seed = 1;

    private static final TIntHashSet workSet = new TIntHashSet();

    private static void populateRandomBlockValues(
            final Random random, final int[] blockValues, final int valuesPerBlock) {
        workSet.clear();
        while (workSet.size() < valuesPerBlock) {
            workSet.add(random.nextInt(RspArray.BLOCK_SIZE));
        }
        workSet.toArray(blockValues);
        Arrays.sort(blockValues);
    }

    @Setup(Level.Trial)
    public void setupEnv() {
        final Random random = new Random(seed);
        values = new long[cardinality];
        int n = 0;
        long block = 0;
        final int[] blockValues = new int[valuesPerBlock];
        ADDING_VALUES: while (true) {
            populateRandomBlockValues(random, blockValues, valuesPerBlock);
            for (long v : blockValues) {
                values[n++] = block * RspArray.BLOCK_SIZE + v;
                if (n == cardinality) {
                    break ADDING_VALUES;
                }
            }
            ++block;
        }
    }

    @Benchmark
    public void createTreeIndexImplViaBuilder(Blackhole bh) {
        OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        for (long v : values) {
            builder.appendKey(v);
        }
        bh.consume(builder.getTreeIndexImpl());
    }

    @Benchmark
    public void createRspViaRspBuilder(Blackhole bh) {
        RspBitmapBuilderSequential builder = new RspBitmapBuilderSequential();
        for (long v : values) {
            builder.appendKey(v);
        }
        bh.consume(builder.getTreeIndexImpl());
    }

    @Benchmark
    public void createRspManualInitialEmpty(Blackhole bh) {
        RspBitmap rb = new RspBitmap();
        for (long v : values) {
            rb = rb.add(v);
        }
        bh.consume(rb);
    }

    @Benchmark
    public void createSortedRangesManualPreallocInt(Blackhole bh) {
        SortedRanges sr = new SortedRangesInt(cardinality, 0);
        for (long v : values) {
            sr = sr.append(v);
        }
        bh.consume(sr);
    }

    @Benchmark
    public void createSortedRangesManualTwoInitialcapacityInt(Blackhole bh) {
        SortedRanges sr = new SortedRangesInt(2, 0);
        for (long v : values) {
            sr = sr.append(v);
        }
        bh.consume(sr);
    }

    @Benchmark
    public void createSortedRangesManualPreallocLong(Blackhole bh) {
        SortedRanges sr = new SortedRangesLong(cardinality);
        for (long v : values) {
            sr = sr.append(v);
        }
        bh.consume(sr);
    }

    @Benchmark
    public void createSortedRangesDefaultInitialCapacityLong(Blackhole bh) {
        SortedRanges sr = new SortedRangesLong();
        for (long v : values) {
            sr = sr.append(v);
        }
        bh.consume(sr);
    }

    @Benchmark
    public void fillAnArrayOfSameSizeOneElementAtATime(Blackhole bh) {
        final long[] vs = new long[cardinality];
        for (int i = 0; i < cardinality; ++i) {
            vs[i] = values[i];
        }
        bh.consume(vs);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SmallRowSetCreation.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
