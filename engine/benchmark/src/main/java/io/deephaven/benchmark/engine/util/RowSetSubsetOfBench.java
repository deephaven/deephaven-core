//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmark.engine.util.RowSetShapes.Pattern;
import io.deephaven.benchmark.engine.util.RowSetShapes.Representation;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
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
 * {@link OrderedLongSet#ixSubsetOf} across the same shapes, sizes and representations as {@link RowSetOverlapsBench},
 * which it shares its generators with through {@link RowSetShapes}.
 *
 * <p>
 * Subset testing is not symmetric, and unlike overlapping it cannot skip: every range of the subject has to be found in
 * the superset, because any one of them can falsify the answer. So the shape of the cost is the subject's range count
 * times what it takes to locate each one, and what the patterns vary is that second factor -- how far apart the two
 * sides' ranges sit, and how many spans or array positions a search has to cross.
 *
 * <p>
 * The subject is a {@link Pattern}'s first side; the superset is that side unioned with the second, so the pattern
 * decides how the superset's extra ranges interleave with the subject's. {@link Outcome} then decides where the answer
 * is, by dropping one key from the superset.
 *
 * <p>
 * {@link #subAs} and {@link #supAs} name the two sides' representations separately rather than as a pair plus a swap
 * flag, because subset testing is directed: {@code SortedRanges subsetOf RspBitmap} and
 * {@code RspBitmap subsetOf SortedRanges} are different code paths, not two orders of one.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1)
public class RowSetSubsetOfBench {

    /** Where the answer is, and so how much of the subject the walk reads before it knows. */
    public enum Outcome {
        /** Every range of the subject is present, so all of them have to be read. */
        SUBSET,
        /**
         * The superset is missing a key near the end of the subject, so the walk reads almost everything and then
         * fails. The key is the last one the superset's upper bound does not rest on, since dropping that bound would
         * let an {@code O(1)} check answer before the walk began.
         */
        MISSING_LATE,
        /**
         * The superset is missing the subject's second key, so the walk fails at once. The second rather than the
         * first, for the same reason: dropping the first would move the superset's lower bound.
         */
        MISSING_EARLY
    }

    /** How the subject is held. */
    @Param
    private Representation subAs;

    /** How the superset is held. */
    @Param
    private Representation supAs;

    @Param
    private Pattern pattern;

    /**
     * Ranges per side before the union. The superset holds up to twice as many, which is why this stops short of the
     * sizes {@link RowSetOverlapsBench} uses: it has to fit in a {@link SortedRanges} as well.
     */
    @Param({"64", "512", "1024"})
    private int size;

    @Param
    private Outcome outcome;

    private OrderedLongSet sub;
    private OrderedLongSet sup;

    @Setup
    public void setup() {
        final long[][] sides = RowSetShapes.build(pattern, size);
        final long[] subKeys = sides[0];
        long[] supKeys = RowSetShapes.union(subKeys, sides[1]);
        switch (outcome) {
            case SUBSET:
                break;
            case MISSING_LATE:
                supKeys = RowSetShapes.without(supKeys, lateKey(subKeys, supKeys[supKeys.length - 1]));
                break;
            case MISSING_EARLY:
                supKeys = RowSetShapes.without(supKeys, secondKey(subKeys));
                break;
            default:
                throw new IllegalStateException("unhandled outcome " + outcome);
        }
        final boolean expected = RowSetShapes.subsetOf(subKeys, supKeys);
        if (expected != (outcome == Outcome.SUBSET)) {
            throw new IllegalStateException(outcome + " did not produce the answer it is named for, on " + pattern);
        }

        sub = RowSetShapes.impl(subAs, subKeys);
        sup = RowSetShapes.impl(supAs, supKeys);

        if (sub.ixSubsetOf(sup) != expected) {
            throw new IllegalStateException("subsetOf disagrees with the expected answer for " + pattern);
        }
        System.out.println(subAs + "_in_" + supAs + " " + pattern + " size=" + size + " outcome=" + outcome
                + " expected=" + expected + " subRanges=" + RowSetShapes.rangeCount(subKeys)
                + " supRanges=" + RowSetShapes.rangeCount(supKeys)
                + " sub=" + sub.getClass().getSimpleName() + " sup=" + sup.getClass().getSimpleName());
    }

    /** The subject's second key, wherever it falls. */
    private static long secondKey(final long[] ranges) {
        if (ranges[1] > ranges[0]) {
            return ranges[0] + 1;
        }
        return ranges[2];
    }

    /**
     * The subject's last key that the superset's upper bound does not rest on. When the two end together, step back a
     * key so that removing it leaves the bound where it was and the walk, rather than the bound check, has to answer.
     */
    private static long lateKey(final long[] sub, final long supLast) {
        final long last = sub[sub.length - 1];
        if (last < supLast) {
            return last;
        }
        if (last > sub[sub.length - 2]) {
            return last - 1;
        }
        return sub[sub.length - 3];
    }

    @Benchmark
    public boolean subsetOf() {
        return sub.ixSubsetOf(sup);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetSubsetOfBench.class);
    }
}
