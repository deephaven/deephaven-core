/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmark.engine.util;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.rsp.RspIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;
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
@Measurement(iterations = 3, time = 7)
@Fork(value = 1)

public class RowSetIterationBench {

    @Param({"10000000"}) // "10000", "100000", "1000000"}) // , "10000000"})
    private static int sz;
    private long[] values;
    private RspBitmap rb;
    private RowSet ix;
    private TLongHashSet tset;

    @Param({"10"})
    private static int elementStep;

    @Param("3")
    private static int rangeStep;

    @Setup(Level.Trial)
    public void setup() {
        values = new long[sz];
        long prev = 0;
        final Random r = new Random(1);
        rb = new RspBitmap();
        for (int i = 0; i < sz; ++i) {
            final long v = prev + rangeStep + 1 + r.nextInt(elementStep);
            values[i] = v;
            rb.appendUnsafe(v);
            prev = v;
        }
        rb.finishMutationsAndOptimize();
        ix = new WritableRowSetImpl(rb);
        tset = new TLongHashSet(values);
    }

    @Benchmark
    public void b00_iterateLongArray(final Blackhole bh) {
        for (long v : values) {
            bh.consume(v);
        }
    }

    @Benchmark
    public void b01_iterateRspBitmap(final Blackhole bh) {
        final RspIterator it = rb.getIterator();
        while (it.hasNext()) {
            final long v = it.nextLong();
            bh.consume(v);
        }
    }

    @Benchmark
    public void b02_forEachRspBitmap(final Blackhole bh) {
        rb.forEachLong((final long v) -> {
            bh.consume(v);
            return true;
        });
    }

    @Benchmark
    public void b03_iterateIndex(final Blackhole bh) {
        final RowSet.Iterator it = ix.iterator();
        while (it.hasNext()) {
            final long v = it.nextLong();
            bh.consume(v);
        }
    }

    @Benchmark
    public void b04_forEachIndex(final Blackhole bh) {
        ix.forAllRowKeys((final long v) -> bh.consume(v));
    }

    @Benchmark
    public void b05_iterateTroveLongHashSet(final Blackhole bh) {
        final TLongIterator it = tset.iterator();
        while (it.hasNext()) {
            final long v = it.next();
            bh.consume(v);
        }
    }

    @Benchmark
    public void b06_forEachTroveLongHashSet(final Blackhole bh) {
        tset.forEach((final long v) -> {
            bh.consume(v);
            return true;
        });
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetIterationBench.class);
    }
}
