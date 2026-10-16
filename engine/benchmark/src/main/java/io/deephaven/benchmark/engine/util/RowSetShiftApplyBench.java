//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.mutable.MutableLong;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The row set work of collapsing runs of sparse blocks in an aggregation's result, as
 * {@code OutputPositionBlockTracker.collapseSparseBlocks} does each update cycle.
 *
 * <p>
 * The result row set holds live output positions: {@code backgroundLiveFraction} of the positions outside the runs,
 * chosen at random, and {@code runLiveFraction} of the positions inside each of {@code runs} runs of {@code runBlocks}
 * 2048-position blocks. Collapsing a run moves its live positions, in order, to the start of the run. Because the live
 * positions are random, nearly every one is its own range, so the shift has about as many ranges as states moved.
 * </p>
 *
 * <ul>
 * <li>{@link #buildShift()}: build the {@link RowSetShiftData}, reading each run's live positions from the row set as
 * the tracker does.</li>
 * <li>{@link #applyShift()}: apply that shift to the result row set with {@link RowSetShiftData#apply(WritableRowSet)},
 * as the aggregation does today.</li>
 * <li>{@link #removeAndInsertRuns()}: produce the same row set by removing each run's range and inserting the single
 * contiguous range its live positions occupy after the collapse.</li>
 * </ul>
 *
 * <p>
 * The row set to modify is copied before each invocation, outside the measurement.
 * </p>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1)
public class RowSetShiftApplyBench {

    private static final int BLOCK_SIZE = 2048;
    private static final long RANDOM_SEED = 1;

    /** The output positions spanned by the result. */
    @Param({"10000000"})
    private int totalPositions;

    /** The fraction of positions outside the runs that are live. */
    @Param({"0.1"})
    private double backgroundLiveFraction;

    /** The number of runs collapsed in one cycle. */
    @Param({"10"})
    private int runs;

    /** The blocks in each run. */
    @Param({"2", "8"})
    private int runBlocks;

    /** The fraction of positions inside each run that are live. */
    @Param({"0.5", "0.25", "0.1"})
    private double runLiveFraction;

    private RowSet liveStates;
    private long[] runFirst;
    private long[] runLiveCount;
    private RowSetShiftData shift;
    private WritableRowSet target;

    @Setup(Level.Trial)
    public void setupTrial() {
        final Random random = new Random(RANDOM_SEED);
        runFirst = new long[runs];
        runLiveCount = new long[runs];
        final long runLength = (long) runBlocks * BLOCK_SIZE;
        final long spacing = totalPositions / runs;
        for (int ri = 0; ri < runs; ++ri) {
            // runs are block aligned and spread across the table
            runFirst[ri] = (ri * spacing + spacing / 2) / BLOCK_SIZE * BLOCK_SIZE;
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        int ri = 0;
        for (long position = 0; position < totalPositions; ++position) {
            final boolean inRun = ri < runs && position >= runFirst[ri] && position < runFirst[ri] + runLength;
            if (random.nextDouble() < (inRun ? runLiveFraction : backgroundLiveFraction)) {
                builder.appendKey(position);
                if (inRun) {
                    ++runLiveCount[ri];
                }
            }
            if (ri < runs && position == runFirst[ri] + runLength - 1) {
                ++ri;
            }
        }
        liveStates = builder.build();
        shift = buildShift();

        // both ways of collapsing must produce the same row set
        try (final WritableRowSet shifted = liveStates.copy();
                final WritableRowSet replaced = liveStates.copy()) {
            shift.apply(shifted);
            replaceRuns(replaced);
            if (!shifted.equals(replaced)) {
                throw new IllegalStateException("Collapsing by shift and by replacement disagree");
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        liveStates.close();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        target = liveStates.copy();
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        target.close();
    }

    @Benchmark
    public RowSetShiftData buildShift() {
        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        final long runLength = (long) runBlocks * BLOCK_SIZE;
        for (int ri = 0; ri < runs; ++ri) {
            try (final RowSet runStates = liveStates.subSetByKeyRange(runFirst[ri], runFirst[ri] + runLength - 1)) {
                final MutableLong destination = new MutableLong(runFirst[ri]);
                runStates.forAllRowKeyRanges((first, last) -> {
                    if (first != destination.get()) {
                        shiftBuilder.shiftRange(first, last, destination.get() - first);
                    }
                    destination.add(last - first + 1);
                });
            }
        }
        return shiftBuilder.build();
    }

    @Benchmark
    public WritableRowSet applyShift() {
        shift.apply(target);
        return target;
    }

    @Benchmark
    public WritableRowSet removeAndInsertRuns() {
        replaceRuns(target);
        return target;
    }

    private void replaceRuns(final WritableRowSet rowSet) {
        final long runLength = (long) runBlocks * BLOCK_SIZE;
        for (int ri = 0; ri < runs; ++ri) {
            rowSet.removeRange(runFirst[ri], runFirst[ri] + runLength - 1);
            if (runLiveCount[ri] > 0) {
                rowSet.insertRange(runFirst[ri], runFirst[ri] + runLiveCount[ri] - 1);
            }
        }
    }

    public static void main(String[] args) {
        BenchUtil.run(RowSetShiftApplyBench.class);
    }
}
