//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.rsp.RspIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
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
    private LongSet tset;

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
        tset = new LongOpenHashSet(values);
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
    public void b05_iterateLongOpenHashSet(final Blackhole bh) {
        final LongIterator it = tset.iterator();
        while (it.hasNext()) {
            final long v = it.nextLong();
            bh.consume(v);
        }
    }

    @Benchmark
    public void b06_forEachLongOpenHashSet(final Blackhole bh) {
        tset.forEach((final long v) -> bh.consume(v));
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetIterationBench.class);
    }
}
