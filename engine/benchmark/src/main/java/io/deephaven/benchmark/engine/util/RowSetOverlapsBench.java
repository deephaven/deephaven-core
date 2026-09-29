//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmark.engine.util.RowSetShapes.Layout;
import io.deephaven.benchmark.engine.util.RowSetShapes.Pattern;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

/**
 * {@link OrderedLongSet#ixOverlaps} across the shapes, sizes and representations that decide whether the two cursors
 * seek or one of them walks range by range.
 *
 * <p>
 * The cost of an overlap test is set by where the answer is and how the two sets interleave on the way there. The
 * {@link Pattern}s in {@link RowSetShapes} cover both: {@link Pattern#TOUCH_AT_START} answers immediately and measures
 * only the fixed overhead, while the patterns that answer at the end or not at all are where a walk pays per range and
 * a seek pays per alternation.
 *
 * <p>
 * {@link #swapped} tests the same pair with the arguments the other way around. Overlapping is symmetric, so for a
 * symmetric implementation the two orders cost the same; where they do not, the caller's argument order is a
 * performance variable.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1)
public class RowSetOverlapsBench {

    @Param
    private Layout layout;

    @Param
    private Pattern pattern;

    /** Ranges per side, or on {@link Pattern#DENSE_VS_SPARSE} the count on the sparse side. */
    @Param({"64", "512", "2048"})
    private int size;

    /** Whether to pass the two sides in the other order. */
    @Param({"false", "true"})
    private boolean swapped;

    private OrderedLongSet left;
    private OrderedLongSet right;

    @Setup
    public void setup() {
        final long[][] sides = RowSetShapes.build(pattern, size);
        final long[] a = sides[0];
        final long[] b = sides[1];
        final boolean expected = RowSetShapes.overlaps(a, b);

        final OrderedLongSet[] impls = RowSetShapes.impls(layout, a, b);
        left = swapped ? impls[1] : impls[0];
        right = swapped ? impls[0] : impls[1];

        if (left.ixOverlaps(right) != expected) {
            throw new IllegalStateException("overlaps disagrees with the expected answer for " + pattern);
        }
        System.out.println(layout + " " + pattern + " size=" + size + " swapped=" + swapped
                + " expected=" + expected + " left=" + left.getClass().getSimpleName()
                + " right=" + right.getClass().getSimpleName());
    }

    @Benchmark
    public boolean overlaps() {
        return left.ixOverlaps(right);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetOverlapsBench.class);
    }
}
