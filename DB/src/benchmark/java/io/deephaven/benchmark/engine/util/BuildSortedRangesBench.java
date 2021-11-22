package io.deephaven.benchmark.engine.util;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
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
@Measurement(iterations = 3, time = 10)
@Fork(value = 1)

public class BuildSortedRangesBench {

    private static final int count = 415 * 1000; // -Xmx768M makes for a tight heap, which is how we want to test.
    private static final int sortedRangesIntMaxCapacity = SortedRanges.MAX_CAPACITY / 2;
    private static final int sz = sortedRangesIntMaxCapacity - 2;
    private static final long[] values = new long[sz + 1];
    private static final int step = 10;
    private static final float rangeP = 0.5F;

    private static final OrderedLongSet[] tixs = new OrderedLongSet[count];

    @Setup(Level.Trial)
    public void setup() {
        final Random rand = new Random(1);
        int prev = 0;
        for (int i = 0; i < sz; i += 2) {
            final int randValue = 1 + rand.nextInt(step);
            final int nextStart = prev + randValue;
            if (prev != 0 && rand.nextFloat() < rangeP) {
                final int nextEnd = nextStart + 1 + rand.nextInt(step);
                values[i] = nextStart;
                values[i + 1] = nextEnd;
                prev = nextEnd;
            } else {
                values[i] = values[i + 1] = nextStart;
                prev = nextStart;
            }
        }
    }

    // See "Numerical Recipes in C", 7-1 "Uniform Deviates", "An Even Quicker Generator".
    private static class QuickDirtyRandom {
        private int idum;

        public QuickDirtyRandom(final int seed) {
            idum = seed;
        }

        public QuickDirtyRandom() {
            idum = 1;
        }

        public void next() {
            idum = 1664525 * idum + 1013904223;
        }

        public int curr() {
            return idum;
        }

        public void reset(final int seed) {
            idum = seed;
        }
    }

    private static final QuickDirtyRandom quickRand = new QuickDirtyRandom(1);

    @Benchmark
    public void b03_buildSr(final Blackhole bh) {
        quickRand.reset(1);
        for (int i = 0; i < count; ++i) {
            SortedRanges sr = SortedRanges.makeEmpty();
            int d = 0;
            quickRand.next();
            final int sizeBound = quickRand.curr() & 0x1FF;
            final int n = Math.min(sz, sizeBound);
            for (int j = 0; j < n; j += 2) {
                quickRand.next();;
                d += quickRand.curr() & 0xF;
                sr = sr.appendRange(values[j] + d, values[j + 1] + d);
            }
            sr.tryCompactUnsafe(4);
            tixs[i] = sr;
            bh.consume(tixs[i]);
        }
    }

    @Benchmark
    public void b03_buildRsp(final Blackhole bh) {
        quickRand.reset(1);
        for (int i = 0; i < count; ++i) {
            RspBitmap rb = RspBitmap.makeEmpty();
            int d = 0;
            quickRand.next();
            final int sizeBound = quickRand.curr() & 0x1FF;
            final int n = Math.min(sz, sizeBound);
            for (int j = 0; j < n; j += 2) {
                quickRand.next();;
                d += quickRand.curr() & 0xF;

                rb.appendRangeUnsafeNoWriteCheck(values[j] + d, values[j + 1] + d);
            }
            rb.tryCompactUnsafe(4);
            rb.finishMutations();
            tixs[i] = rb;
            bh.consume(tixs[i]);
        }
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(BuildSortedRangesBench.class);
    }
}
