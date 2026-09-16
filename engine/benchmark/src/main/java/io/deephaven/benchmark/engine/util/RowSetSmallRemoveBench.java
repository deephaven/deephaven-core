//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
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
 * A few scattered row keys removed from a row set that holds many scattered row keys, the way an incremental
 * naturalJoin's per-slot left row set loses the rows a cycle moved off that key.
 *
 * <p>
 * The target holds {@code targetKeys} single keys spread uniformly over a key space wide enough to need int offsets, so
 * it is a {@code SortedRangesInt} up to that class's capacity and an {@code RspBitmap} beyond it. The
 * {@code removeKeys} removed keys are a uniformly random subset of the target's keys. {@link #bulkRemove} is one
 * {@link WritableRowSet#remove(io.deephaven.engine.rowset.RowSet)} call; {@link #forAllRowKeysRemove} removes the same
 * keys one at a time through the row set API, which is the bound the bulk call should never be slower than;
 * {@link #arrayRemove} is the same loop over a primitive array, the floor without the removed set's iteration.
 * </p>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1)
public class RowSetSmallRemoveBench {

    private static final long RANDOM_SEED = 1;
    /** Wide enough that offsets need ints, narrow enough that they never need longs. */
    private static final long KEY_SPACE = 1L << 30;

    @Param({"20", "200", "2000", "6000", "20000"})
    private int targetKeys;

    @Param({"1", "2", "5", "20", "100"})
    private int removeKeys;

    private long[] targetSortedKeys;
    private WritableRowSet removed;
    private long[] removedKeys;
    private WritableRowSet target;

    @Setup(Level.Trial)
    public void setupTrial() {
        final Random random = new Random(RANDOM_SEED);
        targetSortedKeys = distinctSortedKeys(random, targetKeys);
        // A uniformly random subset of the target's keys, by a partial Fisher-Yates shuffle of their positions.
        final int[] positions = new int[targetKeys];
        for (int i = 0; i < targetKeys; ++i) {
            positions[i] = i;
        }
        final int chosen = Math.min(removeKeys, targetKeys);
        for (int i = 0; i < chosen; ++i) {
            final int j = i + random.nextInt(targetKeys - i);
            final int tmp = positions[i];
            positions[i] = positions[j];
            positions[j] = tmp;
        }
        final RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();
        for (int i = 0; i < chosen; ++i) {
            removedBuilder.addKey(targetSortedKeys[positions[i]]);
        }
        removed = removedBuilder.build();
        removedKeys = new long[chosen];
        removed.asRowKeyChunk().copyToArray(0, removedKeys, 0, chosen);
        try (final WritableRowSet base = buildTarget()) {
            System.out.println("targetKeys=" + targetKeys + " target="
                    + ((WritableRowSetImpl) base).getInnerSet().getClass().getSimpleName()
                    + " removeKeys=" + chosen + " removed="
                    + ((WritableRowSetImpl) removed).getInnerSet().getClass().getSimpleName());
        }
    }

    /** {@code count} distinct keys uniformly drawn from the key space. */
    private static long[] distinctSortedKeys(final Random random, final int count) {
        try (final WritableRowSet drawn = RowSetFactory.empty()) {
            while (drawn.size() < count) {
                drawn.insert((long) (random.nextDouble() * KEY_SPACE));
            }
            final long[] keys = new long[count];
            drawn.asRowKeyChunk().copyToArray(0, keys, 0, count);
            return keys;
        }
    }

    /**
     * A fresh, privately owned target. {@link WritableRowSet#copy()} shares the inner set copy-on-write, and the first
     * removal from such a copy would pay a full deep copy inside the timed region, so the target is rebuilt instead.
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
    public long bulkRemove() {
        target.remove(removed);
        return target.size();
    }

    @Benchmark
    public long forAllRowKeysRemove() {
        removed.forAllRowKeys(target::remove);
        return target.size();
    }

    @Benchmark
    public long arrayRemove() {
        for (final long key : removedKeys) {
            target.remove(key);
        }
        return target.size();
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetSmallRemoveBench.class);
    }
}
