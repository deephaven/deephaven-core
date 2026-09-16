//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A few scattered row keys inserted into a row set that already holds many scattered row keys, the way an incremental
 * naturalJoin's per-slot left row set receives the rows a cycle moved onto that key.
 *
 * <p>
 * The target holds {@code targetKeys} single keys spread uniformly over a key space wide enough to need int offsets, so
 * it is a {@code SortedRangesInt} up to that class's capacity and an {@code RspBitmap} beyond it. The
 * {@code insertKeys} added keys are uniformly random positions not already present, built with a sequential builder as
 * the join's slot tracker builds them. {@link #bulkInsert} is one
 * {@link WritableRowSet#insert(io.deephaven.engine.rowset.RowSet)} call; {@link #forAllRowKeysInsert} inserts the same
 * keys one at a time through the row set API, which is the bound the bulk call should never be slower than;
 * {@link #arrayInsert} is the same loop over a primitive array, the floor without the added set's iteration.
 * </p>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1)
public class RowSetSmallInsertBench {

    private static final long RANDOM_SEED = 1;
    /** Wide enough that offsets need ints, narrow enough that they never need longs. */
    private static final long KEY_SPACE = 1L << 30;

    @Param({"20", "200", "2000", "6000", "20000"})
    private int targetKeys;

    @Param({"1", "2", "5", "20", "100"})
    private int insertKeys;

    private long[] targetSortedKeys;
    private WritableRowSet base;
    private WritableRowSet added;
    private long[] addedKeys;
    private WritableRowSet target;

    @Setup(Level.Trial)
    public void setupTrial() {
        final Random random = new Random(RANDOM_SEED);
        targetSortedKeys = distinctSortedKeys(random, targetKeys, null);
        base = buildTarget();
        addedKeys = distinctSortedKeys(random, insertKeys, base);
        final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
        for (final long key : addedKeys) {
            addedBuilder.appendKey(key);
        }
        added = addedBuilder.build();
        System.out.println("targetKeys=" + targetKeys + " target="
                + ((WritableRowSetImpl) base).getInnerSet().getClass().getSimpleName()
                + " insertKeys=" + insertKeys + " added="
                + ((WritableRowSetImpl) added).getInnerSet().getClass().getSimpleName());
    }

    /** {@code count} distinct keys uniformly drawn from the key space, excluding those already in {@code exclude}. */
    private static long[] distinctSortedKeys(final Random random, final int count, final WritableRowSet exclude) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        try (final WritableRowSet drawn = RowSetFactory.empty()) {
            while (drawn.size() < count) {
                final long key = (long) (random.nextDouble() * KEY_SPACE);
                if (exclude != null && exclude.containsRange(key, key)) {
                    continue;
                }
                drawn.insert(key);
            }
            final long[] keys = new long[count];
            drawn.asRowKeyChunk().copyToArray(0, keys, 0, count);
            return keys;
        }
    }

    /**
     * A fresh, privately owned target. {@link WritableRowSet#copy()} shares the inner set copy-on-write, and the first
     * insert into such a copy would pay a full deep copy inside the timed region, so the target is rebuilt instead.
     */
    private WritableRowSet buildTarget() {
        final RowSetBuilderSequential targetBuilder = RowSetFactory.builderSequential();
        for (final long key : targetSortedKeys) {
            targetBuilder.appendKey(key);
        }
        return targetBuilder.build();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        target = buildTarget();
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        target.close();
    }

    @Benchmark
    public long bulkInsert() {
        target.insert(added);
        return target.size();
    }

    @Benchmark
    public long forAllRowKeysInsert() {
        added.forAllRowKeys(target::insert);
        return target.size();
    }

    @Benchmark
    public long arrayInsert() {
        for (final long key : addedKeys) {
            target.insert(key);
        }
        return target.size();
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetSmallInsertBench.class);
    }
}
