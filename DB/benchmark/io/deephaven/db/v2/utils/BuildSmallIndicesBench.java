package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 10, time = 3)
@Fork(value = 1)

public class BuildSmallIndicesBench {

    private static final int sz = 10 * 1000 * 1000;
    private final long[] values = new long[sz];

    @Setup(Level.Trial)
    public void setup() {
        final Random r = new Random(1);
        for (int i = 0; i < sz; ++i) {
            values[i] = r.nextInt();
        }
    }

    // If we create a builder that just lives through the scope of the for loop,
    // things are much faster (but less realistic for the case we are trying to simulate)
    // since the JVM can reuse the object skeleton and avoid much of the construction cost
    // given the object doesn't scape. We create an external array to avoid that.
    private final RspBitmap[] rixs = new RspBitmap[sz];

    @Benchmark
    public void b00_buildAndPopulateRspByInsert(final Blackhole bh) {
        for (int i = 0; i < sz; ++i) {
            rixs[i] = new RspBitmap();
            rixs[i].ixInsert(values[i]);
            bh.consume(rixs[i]);
        }
    }

    @Benchmark
    public void b01_buildAndPopulateRspBySpecialConstructor(final Blackhole bh) {
        for (int i = 0; i < sz; ++i) {
            rixs[i] = new RspBitmap(values[i], values[i]);
            bh.consume(rixs[i]);
        }
    }

    private final SingleRange[] sixs = new SingleRange[sz];

    @Benchmark
    public void b02_buildAndPopulateSingleRangeIndex(final Blackhole bh) {
        for (int i = 0; i < sz; ++i) {
            sixs[i] = SingleRange.make(values[i], values[i]);
            bh.consume(sixs[i]);
        }
    }

    private final Index.SequentialBuilder[] sbs = new Index.SequentialBuilder[sz];

    @Benchmark
    public void b03_buildAndPopulateWithIndexBuilder(final Blackhole bh) {
        for (int i = 0; i < sz; ++i) {
            sbs[i] = Index.FACTORY.getSequentialBuilder();
            sbs[i].appendKey(values[i]);
            bh.consume(sbs[i].getIndex());
        }
    }

    private final long[][] ls = new long[sz][];

    @Benchmark
    public void b04_buildAndPopulateArray(final Blackhole bh) {
        for (int i = 0; i < sz; ++i) {
            ls[i] = new long[1];
            ls[i][0] = values[i];
            bh.consume(ls[i][0]);
        }
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(BuildSmallIndicesBench.class);
    }
}
