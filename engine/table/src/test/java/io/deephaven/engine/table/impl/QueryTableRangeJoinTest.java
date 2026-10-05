//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Aggregations;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.OperationException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.deephaven.engine.util.TableTools;
import io.deephaven.vector.IntVector;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.table.TableFactory.emptyTable;
import static io.deephaven.engine.testutil.TstUtils.sparsify;
import static io.deephaven.time.DateTimeUtils.minus;
import static io.deephaven.time.DateTimeUtils.plus;
import static io.deephaven.util.QueryConstants.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the {@link QueryTable#rangeJoin(Table, Collection, RangeJoinMatch, Collection)} and related delegation
 * from {@link io.deephaven.api.TableOperations#rangeJoin(Object, Collection, Collection)}.
 */
@Category(OutOfBandTest.class)
public class QueryTableRangeJoinTest {

    private static final boolean PRINT_EXPECTED_EXCEPTIONS = false;
    private static final int VERIFY_CHUNK_SIZE = 1024;
    private static final int EMPTY = -1;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    // region compare-equal object tests

    /**
     * BigDecimal range values that differ only in scale compare equal, so right rows with 1.0, 1.00, 1.000 and 1 are
     * one run of equal range values: each range includes all of the run or none of it, whatever the scale of the left
     * start and end values.
     */
    @Test
    public void testBigDecimalCompareEqualRightRangeValues() {
        final BigDecimal[] rightValues = {new BigDecimal("0.5"), new BigDecimal("1.0"), new BigDecimal("1.00"),
                new BigDecimal("1.000"), new BigDecimal("1"), new BigDecimal("2")};
        final BigDecimal[] leftStarts = {new BigDecimal("1"), new BigDecimal("0.5"), new BigDecimal("1.0"),
                new BigDecimal("0"), new BigDecimal("1.00"), new BigDecimal("0.7"), new BigDecimal("1.5")};
        final BigDecimal[] leftEnds = {new BigDecimal("2"), new BigDecimal("1.00"), new BigDecimal("1.000"),
                new BigDecimal("3"), new BigDecimal("1"), new BigDecimal("1.5"), new BigDecimal("1.7")};
        final Table right = TableTools.newTable(
                TableTools.col("RRV", rightValues),
                TableTools.intCol("Sentinel", 0, 1, 2, 3, 4, 5));
        final Table left = TableTools.newTable(
                TableTools.col("LSV", leftStarts),
                TableTools.col("LEV", leftEnds));

        for (final boolean startInclusive : new boolean[] {false, true}) {
            for (final boolean endInclusive : new boolean[] {false, true}) {
                final String match = "LSV " + (startInclusive ? "<=" : "<") + " RRV " + (endInclusive ? "<=" : "<")
                        + " LEV";
                final Table result = left.rangeJoin(right, List.of(match), List.of(AggGroup("Sentinel")));
                for (int li = 0; li < leftStarts.length; ++li) {
                    final List<Integer> expected = new ArrayList<>();
                    for (int ri = 0; ri < rightValues.length; ++ri) {
                        final int startComparison = leftStarts[li].compareTo(rightValues[ri]);
                        final int endComparison = rightValues[ri].compareTo(leftEnds[li]);
                        if ((startInclusive ? startComparison <= 0 : startComparison < 0)
                                && (endInclusive ? endComparison <= 0 : endComparison < 0)) {
                            expected.add(ri);
                        }
                    }
                    checkSentinels(match + ", left row " + li, expected, result, li);
                }
            }
        }

        final String allowMatch = "<- LSV <= RRV <= LEV";
        final Table allowResult = left.rangeJoin(right, List.of(allowMatch), List.of(AggGroup("Sentinel")));
        for (int li = 0; li < leftStarts.length; ++li) {
            int first = 0;
            while (first < rightValues.length && rightValues[first].compareTo(leftStarts[li]) < 0) {
                ++first;
            }
            if (first > 0 && (first == rightValues.length || rightValues[first].compareTo(leftStarts[li]) != 0)) {
                --first;
            }
            final List<Integer> expected = new ArrayList<>();
            for (int ri = first; ri < rightValues.length && rightValues[ri].compareTo(leftEnds[li]) <= 0; ++ri) {
                expected.add(ri);
            }
            checkSentinels(allowMatch + ", left row " + li, expected, allowResult, li);
        }
    }

    private static void checkSentinels(final String context, final List<Integer> expected, final Table result,
            final int rowPosition) {
        final IntVector actual = (IntVector) result.getColumnSource("Sentinel")
                .get(result.getRowSet().get(rowPosition));
        final List<Integer> actualList = new ArrayList<>();
        if (actual != null) {
            for (int ii = 0; ii < actual.size(); ++ii) {
                actualList.add(actual.get(ii));
            }
        }
        assertThat(actualList).as(context).isEqualTo(expected);
    }

    // endregion compare-equal object tests

    // region allow following tests

    /**
     * With GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING, a left end value that exactly matches a right value ends its range at
     * that right value, and a greater left end value with no exact match includes the immediately following right
     * value, whatever the other left rows in the bucket.
     */
    @Test
    public void testAllowFollowingAfterExactMatch() {
        final Table rightInts = TableTools.newTable(
                TableTools.intCol("RRV", 1, 2, 4, 6),
                TableTools.intCol("Sentinel", 0, 1, 2, 3));
        final Table leftInts = TableTools.newTable(
                TableTools.intCol("LSV", 0, 0, 0, 0, 0),
                TableTools.intCol("LEV", 2, 2, 3, 4, 5));
        checkAllowFollowingAfterExactMatch(leftInts, rightInts);

        final Table rightStrings = TableTools.newTable(
                TableTools.stringCol("RRV", "b", "c", "e", "g"),
                TableTools.intCol("Sentinel", 0, 1, 2, 3));
        final Table leftStrings = TableTools.newTable(
                TableTools.stringCol("LSV", "a", "a", "a", "a", "a"),
                TableTools.stringCol("LEV", "c", "c", "d", "e", "f"));
        checkAllowFollowingAfterExactMatch(leftStrings, rightStrings);
    }

    private static void checkAllowFollowingAfterExactMatch(final Table left, final Table right) {
        final String match = "LSV <= RRV <= LEV ->";
        final Table result = left.rangeJoin(right, List.of(match), List.of(AggGroup("Sentinel")));
        checkSentinels(match + ", left row 0", List.of(0, 1), result, 0);
        checkSentinels(match + ", left row 1", List.of(0, 1), result, 1);
        checkSentinels(match + ", left row 2", List.of(0, 1, 2), result, 2);
        checkSentinels(match + ", left row 3", List.of(0, 1, 2), result, 3);
        checkSentinels(match + ", left row 4", List.of(0, 1, 2, 3), result, 4);
    }

    // endregion allow following tests

    // region negative zero tests

    /**
     * Double range values follow Deephaven ordering, where -0.0 and 0.0 are equal, for every range rule.
     */
    @Test
    public void testNegativeZeroDouble() {
        checkDoubleRange("LSV < RRV < LEV", new double[] {-1.0, 0.0, 1.0}, -0.0, 5.0, List.of(2));
        checkDoubleRange("LSV <= RRV < LEV", new double[] {-1.0, -0.0, 1.0}, 0.0, 5.0, List.of(1, 2));
        checkDoubleRange("<- LSV <= RRV < LEV", new double[] {-1.0, -0.0, -0.0, 1.0}, 0.0, 5.0, List.of(1, 2, 3));
        checkDoubleRange("LSV < RRV < LEV", new double[] {-1.0, -0.0, 1.0}, -5.0, 0.0, List.of(0));
        checkDoubleRange("LSV < RRV <= LEV", new double[] {-1.0, 0.0, 1.0}, -5.0, -0.0, List.of(0, 1));
        checkDoubleRange("LSV < RRV <= LEV ->", new double[] {-1.0, 0.0, 1.0}, -5.0, -0.0, List.of(0, 1));
    }

    /**
     * Float range values follow Deephaven ordering, where -0.0 and 0.0 are equal.
     */
    @Test
    public void testNegativeZeroFloat() {
        final Table right = TableTools.newTable(
                TableTools.floatCol("RRV", -1.0f, 0.0f, 1.0f),
                TableTools.intCol("Sentinel", 0, 1, 2));
        final Table left = TableTools.newTable(
                TableTools.floatCol("LSV", -0.0f),
                TableTools.floatCol("LEV", 5.0f));
        final String match = "LSV < RRV < LEV";
        checkSentinels(match, List.of(2), left.rangeJoin(right, List.of(match), List.of(AggGroup("Sentinel"))), 0);
    }

    /**
     * A -0.0 left start or end value gets the same range whatever the position of the equal 0.0 in the right values.
     */
    @Test
    public void testNegativeZeroConsistentAcrossRightPositions() {
        final double[] smallerValues = {-3.0, -2.0, -1.0};
        for (int smallerCount = 0; smallerCount <= smallerValues.length; ++smallerCount) {
            final double[] rightValues = new double[smallerCount + 2];
            System.arraycopy(smallerValues, smallerValues.length - smallerCount, rightValues, 0, smallerCount);
            rightValues[smallerCount] = 0.0;
            rightValues[smallerCount + 1] = 1.0;
            final List<Integer> throughZero = new ArrayList<>();
            for (int ri = 0; ri <= smallerCount; ++ri) {
                throughZero.add(ri);
            }
            checkDoubleRange("LSV < RRV < LEV", rightValues, -0.0, 5.0, List.of(smallerCount + 1));
            checkDoubleRange("LSV <= RRV < LEV", rightValues, -0.0, 5.0, List.of(smallerCount, smallerCount + 1));
            checkDoubleRange("LSV < RRV < LEV", rightValues, -5.0, -0.0, throughZero.subList(0, smallerCount));
            checkDoubleRange("LSV < RRV <= LEV", rightValues, -5.0, -0.0, throughZero);
        }
    }

    private static void checkDoubleRange(final String match, final double[] rightValues,
            final double leftStart, final double leftEnd, final List<Integer> expected) {
        final int[] sentinels = new int[rightValues.length];
        for (int ri = 0; ri < rightValues.length; ++ri) {
            sentinels[ri] = ri;
        }
        final Table right = TableTools.newTable(
                TableTools.doubleCol("RRV", rightValues),
                TableTools.intCol("Sentinel", sentinels));
        final Table left = TableTools.newTable(
                TableTools.doubleCol("LSV", leftStart),
                TableTools.doubleCol("LEV", leftEnd));
        final Table result = left.rangeJoin(right, List.of(match), List.of(AggGroup("Sentinel")));
        checkSentinels(String.format("%s, right %s, left (%s, %s)", match, Arrays.toString(rightValues),
                leftStart, leftEnd), expected, result, 0);
    }

    // endregion negative zero tests

    // region job lifecycle tests

    private static final long JOB_HOLD_SECONDS = 2;
    private static final long JOB_TIMEOUT_SECONDS = 30;

    private static volatile CountDownLatch heldJobEntered;
    private static volatile CountDownLatch heldJobRelease;
    private static final AtomicInteger HELD_JOB_CALLS = new AtomicInteger();
    private static final AtomicInteger HELD_JOBS_RUNNING = new AtomicInteger();

    /**
     * Formula that holds the job evaluating it until {@link #heldJobRelease} is released, which happens at the latest
     * {@link #JOB_HOLD_SECONDS} after the first hold began.
     */
    @SuppressWarnings("unused")
    public static int holdJob(final long rowKey) {
        HELD_JOB_CALLS.incrementAndGet();
        HELD_JOBS_RUNNING.incrementAndGet();
        try {
            heldJobEntered.countDown();
            if (!heldJobRelease.await(JOB_HOLD_SECONDS, TimeUnit.SECONDS)) {
                heldJobRelease.countDown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            HELD_JOBS_RUNNING.decrementAndGet();
        }
        return 0;
    }

    /**
     * Formula that fails once a job is held by {@link #holdJob(long)}.
     */
    @SuppressWarnings("unused")
    public static int failAfterHeldJobEntered(final long rowKey) {
        try {
            // noinspection ResultOfMethodCallIgnored
            heldJobEntered.await(JOB_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throw new IllegalStateException("right range value failure");
    }

    /**
     * When the right table work fails while the left grouping job is running, rangeJoin reports the failure only after
     * that job has finished.
     */
    @Test
    public void testRightFailureWaitsForLeftGroupingJob() throws InterruptedException {
        final int heldJobsRunning = runWithHeldJobs(() -> {
            final Table left = TableTools.emptyTable(10).view(
                    "K = holdJob(ii)", "LSV = (int) ii", "LEV = (int) ii + 2");
            final Table right = TableTools.emptyTable(10).view(
                    "K = (int) 0", "RRV = failAfterHeldJobEntered(ii)", "Sentinel = (int) ii");
            left.rangeJoin(right, List.of("K", "LSV < RRV < LEV"), List.of(AggGroup("Sentinel")));
        }, false, OperationException.class);
        assertThat(heldJobsRunning).as("held jobs running when rangeJoin failed").isZero();
    }

    /**
     * When the thread waiting for rangeJoin is interrupted, rangeJoin starts no further range search tasks, and reports
     * the cancellation only after its running range search tasks have finished.
     */
    @Test
    public void testInterruptWaitsForRangeSearchJobs() throws InterruptedException {
        final int numBuckets = 20;
        final int heldJobsRunning = runWithHeldJobs(() -> {
            final Table left = TableTools.emptyTable(numBuckets).view(
                    "K = (int) ii", "LSV = holdJob(ii)", "LEV = (int) 100");
            final Table right = TableTools.emptyTable(numBuckets).view(
                    "K = (int) ii", "RRV = (int) ii", "Sentinel = (int) ii");
            left.rangeJoin(right, List.of("K", "LSV < RRV < LEV"), List.of(AggGroup("Sentinel")));
        }, true, CancellationException.class);
        assertThat(heldJobsRunning).as("held jobs running when rangeJoin was cancelled").isZero();
        assertThat(HELD_JOB_CALLS.get()).as("range search tasks run").isLessThan(numBuckets);
    }

    /**
     * Run {@code operation} with a parallel operation initializer, expecting it to fail with {@code expectedFailure}.
     * Held jobs are released when {@code operation} returns. If {@code interrupt}, this thread is interrupted once a
     * job is held.
     *
     * @return The number of held jobs running when {@code operation} failed
     */
    private static int runWithHeldJobs(
            @NotNull final Runnable operation,
            final boolean interrupt,
            @NotNull final Class<? extends Exception> expectedFailure) throws InterruptedException {
        heldJobEntered = new CountDownLatch(1);
        heldJobRelease = new CountDownLatch(1);
        HELD_JOB_CALLS.set(0);
        HELD_JOBS_RUNNING.set(0);

        final Thread operationThread = Thread.currentThread();
        final Thread interrupter = new Thread(() -> {
            try {
                if (heldJobEntered.await(JOB_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    operationThread.interrupt();
                }
            } catch (InterruptedException ignored) {
            }
        }, "QueryTableRangeJoinTest-interrupter");
        interrupter.setDaemon(true);

        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, 4);
        final ExecutionContext parallelContext = ExecutionContext.getContext().withOperationInitializer(threadPool);
        int heldJobsRunning = -1;
        try (final SafeCloseable ignored1 = parallelContext.open();
                final SafeCloseable ignored2 = threadPool::shutdown) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(QueryTableRangeJoinTest.class);
            if (interrupt) {
                interrupter.start();
            }
            try {
                operation.run();
                failBecauseExceptionWasNotThrown(expectedFailure);
            } catch (RuntimeException expected) {
                heldJobsRunning = HELD_JOBS_RUNNING.get();
                assertThat(expected).isInstanceOf(expectedFailure);
            }
        } finally {
            heldJobRelease.countDown();
            if (interrupt) {
                interrupter.join(TimeUnit.SECONDS.toMillis(JOB_TIMEOUT_SECONDS));
                if (interrupter.isAlive()) {
                    fail("interrupter thread did not finish");
                }
            }
            // noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
        }
        return heldJobsRunning;
    }

    // endregion job lifecycle tests

    // region validation tests

    @Test
    public void testRefreshingUnsupported() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=II % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=II % 5", "RRV=ii / 0.3");
        final Runnable test = () -> lt.rangeJoin(rt, List.of("BB", "LSV < RRV < LEV"), List.of(AggGroup("II")));

        // {static, static} works
        test.run();

        // {refreshing, static} fails
        lt.setRefreshing(true);
        expectException(test, UnsupportedOperationException.class);

        // {refreshing, refreshing} fails
        rt.setRefreshing(true);
        expectException(test, UnsupportedOperationException.class);

        // {static, refreshing} fails
        lt.setRefreshing(false);
        expectException(test, UnsupportedOperationException.class);
    }

    @Test
    public void testUnsupportedAggregations() {
        final Aggregation[] unsupportedAggs = new Aggregation[] {
                AggAbsSum("II"),
                AggApproxPct(0.1, "II"),
                AggAvg("II"),
                AggCount("Count"),
                AggCountDistinct("II"),
                AggDistinct("II"),
                AggFirst("II"),
                AggFirstRowKey("FRK"),
                AggFormula("each", "each", "II"),
                AggFreeze("II"),
                AggLast("II"),
                AggLastRowKey("LRK"),
                AggMax("II"),
                AggMed("II"),
                AggMin("II"),
                AggPartition("Constituents"),
                AggPct(0.1, "II"),
                AggSortedFirst("BB", "II"),
                AggSortedLast("BB", "II"),
                AggStd("II"),
                AggSum("II"),
                AggTDigest("II"),
                AggUnique("II"),
                AggVar("II"),
                AggWAvg("BB", "II"),
                AggWSum("BB", "II")
        };

        final Table lt = emptyTable(100).updateView("II=ii", "BB=II % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=II % 5", "RRV=ii / 0.3");

        for (final Aggregation agg : unsupportedAggs) {
            expectException(
                    () -> lt.rangeJoin(rt, List.of("BB", "LSV < RRV < LEV"), List.of(agg)),
                    UnsupportedOperationException.class);
        }
    }

    @Test
    public void testUnsupportedAggregationsMessage() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=II % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=II % 5", "RRV=ii / 0.3");
        final List<Aggregation> aggs = List.of(
                AggGroup("G1=II"),
                AggCount("Cnt"),
                AggGroup("G2=II"),
                AggSum("S=II"),
                Aggregations.builder().addAggregations(AggGroup("G3=II"), AggMax("M=II")).build(),
                Aggregations.builder().addAggregations(AggGroup("G4=II"), AggGroup("G5=II")).build());
        assertThatThrownBy(() -> lt.rangeJoin(rt, List.of("BB", "LSV < RRV < LEV"), aggs))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageEndingWith("unsupported aggregations were requested: " + Strings.ofAggregations(List.of(
                        aggs.get(1), aggs.get(3), aggs.get(4))));
    }

    @Test
    public void testInvalidExactMatches() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=i % 5", "RRV=ii / 0.3");
        final Runnable[] tests = new Runnable[] {
                // Missing both
                () -> lt.rangeJoin(rt, List.of("WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left
                () -> lt.rangeJoin(rt, List.of("WRONG=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right
                () -> lt.rangeJoin(rt, List.of("BB=WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both after valid
                () -> lt.rangeJoin(rt, List.of("II", "WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left after valid
                () -> lt.rangeJoin(rt, List.of("II", "WRONG=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right after valid
                () -> lt.rangeJoin(rt, List.of("II", "BB=WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both before valid
                () -> lt.rangeJoin(rt, List.of("WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left before valid
                () -> lt.rangeJoin(rt, List.of("WRONG=BB", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right before valid
                () -> lt.rangeJoin(rt, List.of("BB=WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both and bad data type
                () -> lt.rangeJoin(rt, List.of("WRONG", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left and bad data type
                () -> lt.rangeJoin(rt, List.of("WRONG=BB", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right and bad data type
                () -> lt.rangeJoin(rt, List.of("BB=WRONG", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both and bad component type
                () -> lt.updateView("II=Long.toString(II)").groupBy("BB")
                        .rangeJoin(rt.updateView("II=(CharSequence) Long.toString(II)").groupBy("BB"),
                                List.of("WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left and bad component type (none for left)
                () -> lt.rangeJoin(rt.updateView("II=(CharSequence) Long.toString(II)").groupBy("BB"),
                        List.of("WRONG=BB", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right and bad component type (none for right)
                () -> lt.updateView("II=Long.toString(II)").groupBy("BB")
                        .rangeJoin(rt, List.of("BB=WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
        };
        for (final Runnable test : tests) {
            expectException(test, IllegalArgumentException.class);
        }
    }

    @Test
    public void testInvalidRangeMatch() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=i % 5", "RRV=ii / 0.3");
        final Runnable[] tests = new Runnable[] {
                // Missing all
                () -> lt.rangeJoin(rt, List.of("WRONG1 < WRONG < WRONG2"), List.of(AggGroup("II"))),
                // Incompatible types
                () -> lt.rangeJoin(rt.updateView("RRV=(int) RRV"), List.of("LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Invalid type
                () -> lt.updateView("LSV=new double[] {LSV}", "LEV=new double[] {LEV}")
                        .rangeJoin(rt.updateView("RRV = new double[] {RRV}"),
                                List.of("LSV < RRV < LEV"), List.of(AggGroup("II"))),
        };
        for (final Runnable test : tests) {
            expectException(test, IllegalArgumentException.class);
        }
    }

    @Test
    public void testMissingRangeMatchColumnsNamed() {
        final Table lt = emptyTable(100).updateView("II=ii", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "RRV=ii / 0.3");
        assertThatThrownBy(() -> lt.rangeJoin(rt, List.of("WRONG < RRV < LEV"), List.of(AggGroup("II"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("left start column WRONG is missing");
        assertThatThrownBy(() -> lt.rangeJoin(rt, List.of("LSV < WRONG < LEV"), List.of(AggGroup("II"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("right range column WRONG is missing");
        assertThatThrownBy(() -> lt.rangeJoin(rt, List.of("LSV < RRV < WRONG"), List.of(AggGroup("II"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("left end column WRONG is missing")
                .hasMessageNotContaining("left start column");
    }

    private static void expectException(
            @NotNull final Runnable test,
            @NotNull final Class<? extends Exception> exceptionClass) {
        try {
            test.run();
            failBecauseExceptionWasNotThrown(exceptionClass);
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(exceptionClass);
            if (PRINT_EXPECTED_EXCEPTIONS) {
                expected.printStackTrace(System.out);
            }
        }
    }

    // endregion validation tests

    private static final Instant NOW = Instant.now();

    private enum Type {
        // @formatter:off
        CHAR("char", "io.deephaven.vector.CharVectorDirect.ZERO_LENGTH_VECTOR",
                new char[]{NULL_CHAR, '?', 'B', 'D', 'F', 'H', 'J'},
                new char[]{NULL_CHAR, 'A', 'B', 'C', 'E', 'F', 'G', 'H', 'I'}),
        BYTE("byte", "io.deephaven.vector.ByteVectorDirect.ZERO_LENGTH_VECTOR",
                new byte[]{NULL_BYTE, -1, 1, 3, 5, 7, 9},
                new byte[]{NULL_BYTE, 0, 1, 2, 4, 5, 6, 7, 8}),
        SHORT("short", "io.deephaven.vector.ShortVectorDirect.ZERO_LENGTH_VECTOR",
                new short[]{NULL_SHORT, -1, 1, 3, 5, 7, 9},
                new short[]{NULL_SHORT, 0, 1, 2, 4, 5, 6, 7, 8}),
        INT("int", "io.deephaven.vector.IntVectorDirect.ZERO_LENGTH_VECTOR",
                new int[]{NULL_INT, -1, 1, 3, 5, 7, 9 },
                new int[]{NULL_INT, 0, 1, 2, 4, 5, 6, 7, 8}),
        LONG("long", "io.deephaven.vector.LongVectorDirect.ZERO_LENGTH_VECTOR",
                new long[]{NULL_LONG, -1, 1, 3, 5, 7, 9},
                new long[]{NULL_LONG, 0, 1, 2, 4, 5, 6, 7, 8}),
        FLOAT("float", "io.deephaven.vector.FloatVectorDirect.ZERO_LENGTH_VECTOR",
                new float[]{NULL_FLOAT, -1, 1, 3, 5, 7, 9, Float.NaN},
                new float[]{NULL_FLOAT, 0, 1, 2, 4, 5, 6, 7, 8, Float.NaN}),
        DOUBLE("double", "io.deephaven.vector.DoubleVectorDirect.ZERO_LENGTH_VECTOR",
                new double[]{NULL_DOUBLE, -1, 1, 3, 5, 7, 9, Double.NaN},
                new double[]{NULL_DOUBLE, 0, 1, 2, 4, 5, 6, 7, 8, Double.NaN}),
        TIMESTAMP("Instant", "io.deephaven.vector.ObjectVectorDirect.empty()",
                new Instant[]{null, minus(NOW, 1), plus(NOW, 1), plus(NOW, 3), plus(NOW, 5), plus(NOW, 7),
                        plus(NOW, 9)},
                new Instant[]{null, NOW, plus(NOW, 1), plus(NOW, 2), plus(NOW, 4), plus(NOW, 5),
                        plus(NOW, 6), plus(NOW, 7), plus(NOW, 8)}),
        STRING("String", "io.deephaven.vector.ObjectVectorDirect.empty()",
                new String[]{null, ">?@", "DEF", "IJK", "OPQ", "UVW", "[\\]"},
                new String[]{null, "ABC", "DEF", "FGH", "LMN", "OPQ", "RST", "UVW", "XYZ"});

        private static final int NUM_VALID_UNIQUE_RIGHT_VALUES = 8;

        private static final int[] EXPECTED_LT     = new int[] {1, 1,     3, 4, 6, 8, EMPTY, NULL_INT};
        private static final int[] EXPECTED_LEQ    = new int[] {1, 1,     2, 4, 5, 7, EMPTY, NULL_INT};
        private static final int[] EXPECTED_LEQAP  = new int[] {1, 1,     2, 3, 5, 7, 8,     NULL_INT};
        private static final int[] EXPECTED_GT     = new int[] {8, EMPTY, 1, 3, 4, 6, 8,     NULL_INT};
        private static final int[] EXPECTED_GEQ    = new int[] {8, EMPTY, 2, 3, 5, 7, 8,     NULL_INT};
        private static final int[] EXPECTED_GEQAF  = new int[] {8, 1,     2, 4, 5, 7, 8,     NULL_INT};
        // @formatter:on

        private final String className;
        private final String emptyVector;

        private final Object uniqueLeftValues;
        private final int uniqueLeftCount;
        private final Object uniqueRightValues;
        private final int uniqueRightCount;

        Type(@NotNull final String className,
                @NotNull final String emptyVector,
                @NotNull final Object uniqueLeftValues,
                @NotNull final Object uniqueRightValues) {
            this.className = className;
            this.emptyVector = emptyVector;
            this.uniqueLeftValues = uniqueLeftValues;
            uniqueLeftCount = Array.getLength(uniqueLeftValues);
            this.uniqueRightValues = uniqueRightValues;
            uniqueRightCount = Array.getLength(uniqueRightValues);
        }
    }

    @Test
    public void testRangeJoinStaticEmptyRight() {
        // This just makes sure we properly handle an entirely empty right table, and always uses the double kernel. We
        // have plenty of coverage for all kernels with empty right groups in other tests.
        for (final Type type : Type.values()) {
            if (!type.uniqueLeftValues.getClass().getComponentType().isPrimitive()) {
                // We don't really need coverage for ObjectVector here, so don't bother adding more complexity to Type
                continue;
            }
            final Table lt = emptyTable(20_000).update("BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
            final Table rt = emptyTable(0).update("II=(" + type.className + ") ii", "BB=i % 5", "RRV=ii / 0.3");
            final Table rj = lt.rangeJoin(rt, List.of("BB", "LSV <= RRV <= LEV"), List.of(AggGroup("II")));
            TstUtils.assertTableEquals(rj, lt.update("II=" + type.emptyVector));
        }
    }

    @Test
    public void testRangeJoinStaticLtGt() {
        testRangeJoinStatic(Type.EXPECTED_LT, Type.EXPECTED_GT, "LSV < RRV < LEV", true);
    }

    @Test
    public void testRangeJoinStaticLeqGt() {
        testRangeJoinStatic(Type.EXPECTED_LEQ, Type.EXPECTED_GT, "LSV <= RRV < LEV", true);
    }

    @Test
    public void testRangeJoinStaticLeqapGt() {
        testRangeJoinStatic(Type.EXPECTED_LEQAP, Type.EXPECTED_GT, "<- LSV <= RRV < LEV", true);
    }

    @Test
    public void testRangeJoinStaticLtGeq() {
        testRangeJoinStatic(Type.EXPECTED_LT, Type.EXPECTED_GEQ, "LSV < RRV <= LEV", true);
    }

    @Test
    public void testRangeJoinStaticLeqGeq() {
        testRangeJoinStatic(Type.EXPECTED_LEQ, Type.EXPECTED_GEQ, "LSV <= RRV <= LEV", false);
    }

    @Test
    public void testRangeJoinStaticLeqapGeq() {
        testRangeJoinStatic(Type.EXPECTED_LEQAP, Type.EXPECTED_GEQ, "<- LSV <= RRV <= LEV", false);
    }

    @Test
    public void testRangeJoinStaticLtGeqaf() {
        testRangeJoinStatic(Type.EXPECTED_LT, Type.EXPECTED_GEQAF, "LSV < RRV <= LEV ->", true);
    }

    @Test
    public void testRangeJoinStaticLeqGeqaf() {
        testRangeJoinStatic(Type.EXPECTED_LEQ, Type.EXPECTED_GEQAF, "LSV <= RRV <= LEV ->", false);
    }

    @Test
    public void testRangeJoinStaticLeqapGeqaf() {
        testRangeJoinStatic(Type.EXPECTED_LEQAP, Type.EXPECTED_GEQAF, "<- LSV <= RRV <= LEV ->", false);
    }

    private static void testRangeJoinStatic(
            @NotNull final int[] leftIndexToExpectedRangeStartIndex,
            @NotNull final int[] leftIndexToExpectedRangeEndIndex,
            @NotNull final String rangeMatch,
            final boolean anySideExclusive) {
        QueryScope.addParam("expectedStartIndices", leftIndexToExpectedRangeStartIndex);
        QueryScope.addParam("expectedEndIndices", leftIndexToExpectedRangeEndIndex);
        for (final Type type : Type.values()) {
            QueryScope.addParam("ulCount", type.uniqueLeftCount);
            QueryScope.addParam("ulValues", type.uniqueLeftValues);
            QueryScope.addParam("urCount", type.uniqueRightCount);
            QueryScope.addParam("urValues", type.uniqueRightValues);
            final Table lt = emptyTable(200_000L).updateView(
                    // 160 buckets, 100 size 1, 20 size 100, and 40 sized a little under 5000
                    "BB=ii < 100 ? ii : ii < 2100 ? ii % 20 + 100 : ii % 40 + 110",
                    "LSI=i % ulCount",
                    "LSV=ulValues[LSI]",
                    "LEI=(i * 2 + i % 2) % ulCount",
                    "LEV=ulValues[LEI]",
                    anySideExclusive
                            ? "ExpectedFirstRRI=!isNull(LEV) && !isNull(LSV) && LSV >= LEV ? NULL_INT : expectedStartIndices[LSI]"
                            : "ExpectedFirstRRI=!isNull(LEV) && !isNull(LSV) && LSV > LEV ? NULL_INT : expectedStartIndices[LSI]",
                    anySideExclusive
                            ? "ExpectedLastRRI=!isNull(LEV) && !isNull(LSV) && LSV >= LEV ? NULL_INT : expectedEndIndices[LEI]"
                            : "ExpectedLastRRI=!isNull(LEV) && !isNull(LSV) && LSV > LEV ? NULL_INT : expectedEndIndices[LEI]");

            // A few patterns of sorted right data
            final Table rtSingles = emptyTable(160L * type.uniqueRightCount).updateView(
                    // All unique right values, repeated once per bucket
                    "BB=(long) (ii / urCount)",
                    "RRI=i % urCount",
                    "RRV=urValues[RRI]");
            final Table rtDoublesAndEmpties = emptyTable(160L * type.uniqueRightCount).updateView(
                    // All unique right values, repeated twice per even bucket, odd buckets all null and thus empty
                    "BB=(long) (ii / (2 * urCount))",
                    "RRI=(int) (BB % 2 == 0 ? (i / 2) % urCount : 0)",
                    "RRV=urValues[RRI]");
            final Table rtGrowing = emptyTable((20L * 2 + 140L * 20) * type.uniqueRightCount).updateView(
                    // All unique right values, repeated twice for early buckets and twenty times for later buckets
                    "BB=(long) (ii < 20 * 2 * urCount ? ii / (2 * urCount) : ii / (20 * urCount) + 20)",
                    "RRI=(int) (BB < 20 ? (i / 2) % urCount : (i / 20) % urCount)",
                    "RRV=urValues[RRI]");

            final RangeJoinMatch rangeJoinMatch = RangeJoinMatch.parse(rangeMatch);

            // Dense lefts
            joinAndVerify(type, lt, rtSingles, rangeJoinMatch);
            joinAndVerify(type, lt, rtDoublesAndEmpties, rangeJoinMatch);
            joinAndVerify(type, lt, rtGrowing, rangeJoinMatch);

            // Sparse lefts
            final Table ltSparse = sparsify(lt, 2);
            joinAndVerify(type, ltSparse, rtSingles, rangeJoinMatch);
            joinAndVerify(type, ltSparse, rtDoublesAndEmpties, rangeJoinMatch);
            joinAndVerify(type, ltSparse, rtGrowing, rangeJoinMatch);
        }
    }

    private static void joinAndVerify(
            @NotNull final Type type,
            @NotNull final Table left,
            @NotNull final Table right,
            @NotNull final RangeJoinMatch rangeMatch) {
        final String rrvFilter = type == Type.DOUBLE || type == Type.FLOAT
                ? "!isNaN(RRV) && !isNull(RRV)"
                : "!isNull(RRV)";
        final Collection<? extends JoinMatch> exactMatches = List.of(ColumnName.of("BB"));
        final Table result = left.rangeJoin(right, exactMatches, rangeMatch, List.of(AggGroup("RRI", "RRV")))
                .updateView(
                        String.format("FirstRRV=(%s) (RRV == null || RRV.isEmpty() ? null : RRV.get(0))",
                                type.className),
                        String.format("LastRRV=(%s) (RRV == null || RRV.isEmpty() ? null : RRV.get(RRV.size() - 1))",
                                type.className),
                        "FirstRRI=RRI == null ? null : RRI.isEmpty() ? -1 : RRI.get(0)",
                        "LastRRI=RRI == null ? null : RRI.isEmpty() ? -1 : RRI.get(RRI.size() - 1)",
                        "RangeSize=RRI == null ? null : RRI.isEmpty() ? 0 : RRI.size()",
                        "FirstIsPreceding=isNull(LSV) || isNull(FirstRRV) ? null : LSV > FirstRRV",
                        "LastIsFollowing=isNull(LEV) || isNull(LastRRV) ? null : LEV < LastRRV")
                .naturalJoin(right.where(rrvFilter).countBy("RightSize", "BB"), "BB", "RightSize");
        final ColumnSource<Integer> expectedFirstRRISource = result.getColumnSource("ExpectedFirstRRI", int.class);
        final ColumnSource<Integer> expectedLastRRISource = result.getColumnSource("ExpectedLastRRI", int.class);
        final ColumnSource<Integer> actualFirstRRISource = result.getColumnSource("FirstRRI", int.class);
        final ColumnSource<Integer> actualLastRRISource = result.getColumnSource("LastRRI", int.class);
        final ColumnSource<Long> actualRangeSizeSource = result.getColumnSource("RangeSize", long.class);
        final ColumnSource<Boolean> firstIsPrecedingSource = result.getColumnSource("FirstIsPreceding", Boolean.class);
        final ColumnSource<Boolean> lastIsFollowingSource = result.getColumnSource("LastIsFollowing", Boolean.class);
        final ColumnSource<Long> rightSizeSource = result.getColumnSource("RightSize", long.class);
        long rowPosition = 0;
        // @formatter:off
        try (final RowSequence.Iterator rowsIterator = result.getRowSet().getRowSequenceIterator();
             final SharedContext sharedContext = SharedContext.makeSharedContext();
             final ChunkSource.GetContext expectedFirstRRIGetContext = expectedFirstRRISource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext expectedLastRRIGetContext = expectedLastRRISource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext actualFirstRRIGetContext = actualFirstRRISource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext actualLastRRIGetContext = actualLastRRISource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext actualRangeSizeGetContext = actualRangeSizeSource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext firstIsPrecedingGetContext = firstIsPrecedingSource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext lastIsFollowingGetContext = lastIsFollowingSource
                .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext);
             final ChunkSource.GetContext rightSizeGetContext = rightSizeSource
                     .makeGetContext(VERIFY_CHUNK_SIZE, sharedContext)) {
            // @formatter:on
            while (rowsIterator.hasMore()) {
                final RowSequence rowsSlice = rowsIterator.getNextRowSequenceWithLength(VERIFY_CHUNK_SIZE);
                final IntChunk<? extends Values> expectedFirstRRIChunk = expectedFirstRRISource
                        .getChunk(expectedFirstRRIGetContext, rowsSlice).asIntChunk();
                final IntChunk<? extends Values> expectedLastRRIChunk = expectedLastRRISource
                        .getChunk(expectedLastRRIGetContext, rowsSlice).asIntChunk();
                final IntChunk<? extends Values> actualFirstRRIChunk = actualFirstRRISource
                        .getChunk(actualFirstRRIGetContext, rowsSlice).asIntChunk();
                final IntChunk<? extends Values> actualLastRRIChunk = actualLastRRISource
                        .getChunk(actualLastRRIGetContext, rowsSlice).asIntChunk();
                final LongChunk<? extends Values> actualRangeSizeChunk = actualRangeSizeSource
                        .getChunk(actualRangeSizeGetContext, rowsSlice).asLongChunk();
                final ObjectChunk<Boolean, ? extends Values> firstIsPrecedingChunk = firstIsPrecedingSource
                        .getChunk(firstIsPrecedingGetContext, rowsSlice).asObjectChunk();
                final ObjectChunk<Boolean, ? extends Values> lastIsFollowingChunk = lastIsFollowingSource
                        .getChunk(lastIsFollowingGetContext, rowsSlice).asObjectChunk();
                final LongChunk<? extends Values> rightSizeChunk = rightSizeSource
                        .getChunk(rightSizeGetContext, rowsSlice).asLongChunk();

                final int sliceSize = rowsSlice.intSize();
                for (int ii = 0; ii < sliceSize; ++ii) {
                    final int expectedFirstRRI = expectedFirstRRIChunk.get(ii);
                    final int expectedLastRRI = expectedLastRRIChunk.get(ii);
                    final int actualFirstRRI = actualFirstRRIChunk.get(ii);
                    final int actualLastRRI = actualLastRRIChunk.get(ii);
                    final long actualRangeSize = actualRangeSizeChunk.get(ii);
                    final Boolean firstIsPreceding = firstIsPrecedingChunk.get(ii);
                    final Boolean lastIsFollowing = lastIsFollowingChunk.get(ii);
                    final long rightSize = rightSizeChunk.get(ii);

                    try {
                        if (expectedFirstRRI == NULL_INT || expectedLastRRI == NULL_INT) {
                            assertThat(actualFirstRRI).isEqualTo(NULL_INT);
                            assertThat(actualLastRRI).isEqualTo(NULL_INT);
                        } else if (rightSize == 0 || rightSize == NULL_LONG) {
                            assertThat(actualFirstRRI).isEqualTo(EMPTY);
                            assertThat(actualLastRRI).isEqualTo(EMPTY);
                        } else if (expectedFirstRRI == EMPTY || expectedLastRRI == EMPTY) {
                            assertThat(actualFirstRRI).isEqualTo(EMPTY);
                            assertThat(actualLastRRI).isEqualTo(EMPTY);
                        } else {
                            assertThat(actualFirstRRI).isEqualTo(expectedFirstRRI);
                            assertThat(actualLastRRI).isEqualTo(expectedLastRRI);

                            Assert.eqZero(rightSize % Type.NUM_VALID_UNIQUE_RIGHT_VALUES,
                                    "rightSize % Type.NUM_VALID_UNIQUE_RIGHT_VALUES");
                            final long multiplier = rightSize / Type.NUM_VALID_UNIQUE_RIGHT_VALUES;
                            int adjustment = 0;
                            if (Boolean.TRUE.equals(firstIsPreceding)) {
                                assertThat(rangeMatch.rangeStartRule())
                                        .isEqualTo(RangeStartRule.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING);
                                ++adjustment;
                            }
                            if (Boolean.TRUE.equals(lastIsFollowing)) {
                                assertThat(rangeMatch.rangeEndRule())
                                        .isEqualTo(RangeEndRule.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING);
                                ++adjustment;
                            }
                            final int indexRangeSize = actualLastRRI - actualFirstRRI + 1;
                            final long expectedRangeSize = adjustment + (indexRangeSize - adjustment) * multiplier;
                            assertThat(actualRangeSize).isEqualTo(expectedRangeSize);
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError(String.format("Failure for type %s at row position %s",
                                type, rowPosition), e);
                    }
                    ++rowPosition;
                    sharedContext.reset();
                }
            }
        }
    }
}
