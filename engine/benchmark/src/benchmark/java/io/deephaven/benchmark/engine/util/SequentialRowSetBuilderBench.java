package io.deephaven.benchmark.engine.util;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 30)
@Measurement(iterations = 3, time = 30)
@Fork(value = 1)

public class SequentialRowSetBuilderBench {

    @Param({"10000000"}) // "10000", "100000", "1000000"}) // , "10000000"})
    private static int sz;
    private long[] values;

    @Param({"10"})
    private static int elementStep;

    @Param("3")
    private static int rangeStep;

    @Setup(Level.Trial)
    public void setup() {
        values = new long[sz];
        long prev = 0;
        final Random r = new Random(1);
        for (int i = 0; i < sz; ++i) {
            final long v = prev + rangeStep + 1 + r.nextInt(elementStep);
            values[i] = v;
            prev = v;
        }
    }

    @Benchmark
    public void a00_builderAndPopulateLongArray(final Blackhole bh) {
        final long[] arr = new long[sz];
        System.arraycopy(values, 0, arr, 0, sz);
        bh.consume(arr);
    }

    @Benchmark
    public void b00_buildAndPopulateRspAppendKeyIndices(final Blackhole bh) {
        final RspBitmap rb = new RspBitmap();
        for (long v : values) {
            rb.appendUnsafe(v);
        }
        rb.finishMutationsAndOptimize();
        bh.consume(rb);
    }

    @Benchmark
    public void b01_buildAndPopulateWithSequentialBuilderKeyIndices(final Blackhole bh) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < sz; ++i) {
            b.appendKey(values[i]);
        }
        bh.consume(b.build());
    }

    @Benchmark
    public void c00_buildAndPopulateRspAppendKeyRanges(final Blackhole bh) {
        final RspBitmap rb = new RspBitmap();
        for (int i = 0; i < sz; ++i) {
            final long v = values[i];
            rb.appendRangeUnsafeNoWriteCheck(v, v + rangeStep);
        }
        rb.finishMutationsAndOptimize();
        bh.consume(rb);
    }

    @Benchmark
    public void c01_buildAndPopulateWithSequentialBuilderKeyRanges(final Blackhole bh) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < sz; ++i) {
            final long v = values[i];
            b.appendRange(v, v + rangeStep);
        }
        bh.consume(b.build());
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(SequentialRowSetBuilderBench.class);
    }
}
