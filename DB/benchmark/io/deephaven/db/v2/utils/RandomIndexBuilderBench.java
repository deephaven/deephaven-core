package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.RspBitmap;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 30)
@Measurement(iterations = 3, time = 30)
@Fork(value = 1)

public class RandomIndexBuilderBench {

    @Param({"10000000"}) // , "10000000"})
    private static int sz;
    private long[] values;

    @Param({"10"}) // "65536"}) // {"10", "2000", "65536"})
    private static int elementStep;

    @Param({"3"})
    private static int rangeStep;

    @Setup(Level.Trial)
    public void setup() {
        values = new long[sz];
        final Random r = new Random(1);
        for (int i = 0; i < sz; ++i) {
            final long v = r.nextInt(sz) * (long) elementStep + r.nextInt(elementStep);
            values[i] = v;
        }
    }

    @Benchmark
    public void a00_buildAndPopulateLongArrayThenSort(final Blackhole bh) {
        final long[] arr = new long[sz];
        System.arraycopy(values, 0, arr, 0, sz);
        Arrays.sort(arr);
        bh.consume(arr);
    }

    @Benchmark
    public void a01_buildAndPopulateTLongHashSet(final Blackhole bh) {
        final TLongHashSet tset = new TLongHashSet(values);
        bh.consume(tset);
    }

    @Benchmark
    public void b00b_buildAndPopulateRspAddKeyIndicesLoop(final Blackhole bh) {
        final RspBitmap rb = new RspBitmap();
        for (long v : values) {
            rb.addUnsafe(v);
        }
        rb.finishMutationsAndOptimize();
        bh.consume(rb);
    }

    @Benchmark
    public void b01_buildAndPopulateWithRandomBuilderKeyIndices(final Blackhole bh) {
        final Index.RandomBuilder b = Index.FACTORY.getRandomBuilder();
        for (int i = 0; i < sz; ++i) {
            b.addKey(values[i]);
        }
        bh.consume(b.getIndex());
    }

    @Benchmark
    public void c00_buildAndPopulateRspAddKeyRanges(final Blackhole bh) {
        final RspBitmap rb = new RspBitmap();
        for (int i = 0; i < sz; ++i) {
            final long v = values[i];
            rb.addRangeUnsafeNoWriteCheck(v, v + rangeStep);
        }
        rb.finishMutationsAndOptimize();
        bh.consume(rb);
    }

    @Benchmark
    public void c01_buildAndPopulateWithRandomBuilderKeyRanges(final Blackhole bh) {
        final Index.RandomBuilder b = Index.FACTORY.getRandomBuilder();
        for (int i = 0; i < sz; ++i) {
            final long v = values[i];
            b.addRange(v, v + rangeStep);
        }
        bh.consume(b.getIndex());
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RandomIndexBuilderBench.class);
    }
}
