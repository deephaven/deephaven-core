/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class RspBitmapTimeDrivenTest {

    public static final long RANDOM_SEED_BASE_DEFAULT = 10;
    public static final boolean RANDOM_SEED_FROM_TIME = System.getenv("TEST_SEED_FROM_TIME") != null;

    public static final long FAILURE_CHECK_PERIOD_MILLIS = 2 * 1000;
    public static final long LOG_PERIOD_MILLIS;
    static {
        final String s = System.getenv("TEST_LOG_PERIOD_SECONDS");
        if (s == null) {
            LOG_PERIOD_MILLIS = 15 * 1000;
        } else {
            LOG_PERIOD_MILLIS = 1000L * Integer.parseInt(s);
        }
    }

    // See comment about spec value meanings in caller.
    public static final int SPEC_BASE = 6;

    enum TestSequenceMode {
        EXHAUSTIVE, RANDOM
    }

    // We support two modes of operation, controlled by the TEST_MODE environment variable.
    // * TEST_MODE=EXHAUSTIVE:SPLIT_TOTAL,FIRST_PIECE,LAST_PIECE
    // Perform an EXHAUSTIVE (string literal "EXHAUSTIVE") search of the total range of possibilities (search space).
    // Split the space in SPLIT_TOTAL (integer > 0) sub-ranges.
    // Search over sub-ranges FIRST (integer >= 0) and LAST (integer >= 0).
    // For every sub-range to search, a separate worker thread will be started.
    // * TEST_MODE=RANDOM:WORKERS,TIME_BUDGET_SECONDS
    // Perform a RANDOM (string literal "RANDOM") search of the total range of possibilities (search space).
    // Start WORKERS (integer > 0) number of worker threads.
    // Search for TIME_BUDGET_SECONDS (integer > 0) seconds.
    public static final TestSequenceMode TEST_MODE;
    public static final int SPLIT_SEARCH_SPACE_PIECES;
    public static final int SPLIT_SEARCH_SPACE_FIRST_PIECE;
    public static final int SPLIT_SEARCH_SPACE_LAST_PIECE;
    public static final long PER_TEST_TIME_BUDGET_MILLIS;
    public static final int TEST_WORKERS;
    public static final boolean DEFAULT_RANDOM = true;
    static {
        final String s = System.getenv("TEST_MODE");
        if (s == null) {
            if (DEFAULT_RANDOM) {
                // Good for CI / PR checks.
                TEST_MODE = TestSequenceMode.RANDOM;
                SPLIT_SEARCH_SPACE_PIECES = 1;
                SPLIT_SEARCH_SPACE_FIRST_PIECE = 0;
                SPLIT_SEARCH_SPACE_LAST_PIECE = 0;
                PER_TEST_TIME_BUDGET_MILLIS = 2 * 60 * 1000;
                TEST_WORKERS = 1;
            } else {
                // Good for manual checks.
                TEST_MODE = TestSequenceMode.EXHAUSTIVE;
                SPLIT_SEARCH_SPACE_PIECES = 150000;
                SPLIT_SEARCH_SPACE_FIRST_PIECE = 80000;
                SPLIT_SEARCH_SPACE_LAST_PIECE = 80009;
                PER_TEST_TIME_BUDGET_MILLIS = -1;
                TEST_WORKERS = 1;
            }
        } else {
            String[] parts = s.split(":");
            final String invalidMsg = "Invalid TEST_MODE format: \"" + s +
                    "\"; expected one of " +
                    "\"EXHAUSTIVE:SPLIT_TOTAL,FIRST_PIECE,LAST_PIECE\"" +
                    " or " +
                    "\"TEST_MODE=RANDOM:WORKERS,TIME_BUDGET_SECONDS\"";
            if (parts.length != 2) {
                throw new IllegalArgumentException(invalidMsg);
            }
            switch (parts[0].toLowerCase()) {
                case "exhaustive":
                    TEST_MODE = TestSequenceMode.EXHAUSTIVE;
                    PER_TEST_TIME_BUDGET_MILLIS = -1;
                    parts = parts[1].split(",");
                    if (parts.length != 3) {
                        throw new IllegalArgumentException(invalidMsg);
                    }
                    TEST_WORKERS = SPLIT_SEARCH_SPACE_PIECES = Integer.parseInt(parts[0]);
                    SPLIT_SEARCH_SPACE_FIRST_PIECE = Integer.parseInt(parts[1]);
                    SPLIT_SEARCH_SPACE_LAST_PIECE = Integer.parseInt(parts[2]);
                    if (SPLIT_SEARCH_SPACE_PIECES < 1) {
                        throw new IllegalArgumentException(
                                "Can't split the space in " + SPLIT_SEARCH_SPACE_PIECES + " pieces");
                    }
                    if (SPLIT_SEARCH_SPACE_FIRST_PIECE > SPLIT_SEARCH_SPACE_LAST_PIECE ||
                            SPLIT_SEARCH_SPACE_FIRST_PIECE < 0 ||
                            SPLIT_SEARCH_SPACE_LAST_PIECE > SPLIT_SEARCH_SPACE_PIECES - 1) {
                        throw new IllegalArgumentException(
                                "Can't process first=" + SPLIT_SEARCH_SPACE_FIRST_PIECE +
                                        ", last=" + SPLIT_SEARCH_SPACE_LAST_PIECE +
                                        " in " + SPLIT_SEARCH_SPACE_PIECES + " pieces");
                    }
                    break;
                case "random":
                    TEST_MODE = TestSequenceMode.RANDOM;
                    parts = parts[1].split(",");
                    if (parts.length != 2) {
                        throw new IllegalArgumentException(invalidMsg);
                    }
                    TEST_WORKERS = Integer.parseInt(parts[0]);
                    SPLIT_SEARCH_SPACE_PIECES = 1;
                    SPLIT_SEARCH_SPACE_FIRST_PIECE = 0;
                    SPLIT_SEARCH_SPACE_LAST_PIECE = 0;
                    PER_TEST_TIME_BUDGET_MILLIS = 1000L * Long.parseLong(parts[1]);
                    break;
                default:
                    throw new IllegalStateException(invalidMsg);
            }
        }
    }

    public static final int SEARCH_PIECES = SPLIT_SEARCH_SPACE_LAST_PIECE - SPLIT_SEARCH_SPACE_FIRST_PIECE + 1;

    public static final boolean TEST_OR, TEST_AND, TEST_ANDNOT;
    static {
        final boolean testOnlyOr = System.getenv("TEST_ONLY_OR") != null;
        final boolean testOnlyAnd = System.getenv("TEST_ONLY_AND") != null;
        final boolean testOnlyAndNot = System.getenv("TEST_ONLY_ANDNOT") != null;
        int count = 0;
        if (testOnlyOr)
            ++count;
        if (testOnlyAnd)
            ++count;
        if (testOnlyAndNot)
            ++count;
        if (count > 1) {
            throw new IllegalStateException("Can set only one of TEST_ONLY_OR, TEST_ONLY_AND, TEST_ONLY_ANDNOT");
        }
        if (testOnlyOr) {
            TEST_OR = true;
            TEST_AND = false;
            TEST_ANDNOT = false;
        } else if (testOnlyAnd) {
            TEST_OR = false;
            TEST_AND = true;
            TEST_ANDNOT = false;
        } else if (testOnlyAndNot) {
            TEST_OR = false;
            TEST_AND = false;
            TEST_ANDNOT = true;
        } else {
            TEST_OR = TEST_AND = TEST_ANDNOT = true;
        }
    }

    // Each position in the spec array represents one block in a series of consecutive RSP blocks
    // in the result. Position i in the spec array represents the block starting at i*BLOCK_SIZE
    private static OrderedLongSet fromBlockSpec(final int[] spec) {
        final OrderedLongSet.BuilderSequential b = new OrderedLongSetBuilderSequential();
        for (int i = 0; i < spec.length; ++i) {
            final long baseKey = BLOCK_SIZE * (long) i;
            switch (spec[i]) {
                case 0: // Key not present.
                    break;
                case 1: // Singleton container with element zero.
                    b.appendKey(baseKey);
                    break;
                case 2: // Singleton container with element one.
                    b.appendKey(baseKey + 1);
                    break;
                case 3: // Container with elements zero and one.
                    b.appendKey(baseKey);
                    b.appendKey(baseKey + 1);
                    break;
                case 4: // Container with the range [2, BLOCK_LAST]
                    b.appendRange(baseKey + 2, baseKey + BLOCK_LAST);
                    break;
                case 5: // Full block span.
                    b.appendRange(baseKey, baseKey + BLOCK_LAST);
                    break;
                default:
                    throw new IllegalStateException("Unexpected i=" + i + ", spec[i]=" + spec[i]);
            }
        }
        return b.getOrderedLongSet();
    }

    private static String toStr(final int[] spec) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < spec.length; ++i) {
            final long baseKey = BLOCK_SIZE * (long) i;
            sb.append(" ").append(i).append(" *2^16 : ");
            switch (spec[i]) {
                case 0: // Key not present.
                    sb.append("{}");
                    break;
                case 1: // Singleton container with element zero.
                    sb.append("{0}");
                    break;
                case 2: // Singleton container with element one.
                    sb.append("{1}");
                    break;
                case 3: // Container with elements zero and one.
                    sb.append("{0,1}");
                    break;
                case 4: // Container with the range [2, BLOCK_LAST]
                    sb.append("FULL-{0,1}");
                    break;
                case 5: // Full block span.
                    sb.append("FULL");
                    break;
                default:
                    throw new IllegalStateException("Unexpected i=" + i + ", spec[i]=" + spec[i]);
            }
            if (i < spec.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private static boolean areEqual(final OrderedLongSet oset0, final OrderedLongSet oset1) {
        return oset0.ixCardinality() == oset1.ixCardinality() && oset0.ixSubsetOf(oset1);
    }

    // return a^b.
    private static long pow(final int a, int b) {
        long r = 1;
        while (b > 0) {
            r *= a;
            --b;
        }
        return r;
    }

    enum Operation {
        AND_NOT_EQUALS(TEST_ANDNOT), AND_EQUALS(TEST_AND), OR_EQUALS(TEST_OR);

        public final boolean enabled;

        Operation(final boolean enabled) {
            this.enabled = enabled;
        }
    }

    private static final Map<Operation, BiFunction<RspBitmap, RspBitmap, RspBitmap>> rspOps = new HashMap<>();
    static {
        rspOps.put(Operation.AND_NOT_EQUALS, RspBitmap::andNotEquals);
        rspOps.put(Operation.AND_EQUALS, RspBitmap::andEquals);
        rspOps.put(Operation.OR_EQUALS, RspBitmap::orEquals);
    }
    private static final Map<Operation, BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet>> osetOps =
            new HashMap<>();
    static {
        osetOps.put(Operation.AND_NOT_EQUALS, (x, y) -> x.ixCowRef().ixRemove(y));
        osetOps.put(Operation.AND_EQUALS, (x, y) -> x.ixCowRef().ixRetain(y));
        osetOps.put(Operation.OR_EQUALS, (x, y) -> x.ixCowRef().ixInsert(y));
    }

    private static String toTimeStr(final long millis) {
        final long totalDeciSeconds = millis / 100;
        final long totalSeconds = totalDeciSeconds / 10;
        final long deciSeconds = totalDeciSeconds % 10;
        final long totalMinutes = totalSeconds / 60;
        final long seconds = totalSeconds % 60;
        final long totalHours = totalMinutes / 60;
        final long minutes = totalMinutes % 60;
        final long totalDays = totalHours / 24;
        final long hours = totalHours % 24;
        if (totalDays == 0) {
            if (hours == 0) {
                return String.format("%dm%d.%ds", totalMinutes, seconds, deciSeconds);
            }
            return String.format("%dh%dm%d.%ds", totalHours, minutes, seconds, deciSeconds);
        }
        return String.format("%dd%dh%dm%d.%ds", totalDays, hours, minutes, seconds, deciSeconds);
    }

    private static void valueToSpec(final long value, final int[] spec) {
        final int nblocks = spec.length;
        long v = value + 1; // transform: input is assumed to be offset to avoid the all zeros (empty) case.
        int i = nblocks - 1;
        while (i >= 0) {
            spec[i] = (int) (v % SPEC_BASE);
            v = v / SPEC_BASE;
            --i;
            if (v == 0) {
                break;
            }
        }
        if (v != 0) {
            throw new IllegalStateException("value=" + value + " outside of range for nblocks=" + nblocks);
        }
        for (int j = i; j >= 0; --j) {
            spec[j] = 0;
        }
    }

    private static void log(final String format, final Object... args) {
        System.out.printf(format, args);
        System.out.flush();
    }

    static class TestWorker implements Runnable {
        public static volatile String failure = null; // if not null we discovered a failure.

        private final String workerName;
        private final String testName;
        private final Operation op;
        private final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp;
        private final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp;
        private final int nblocks;
        private final int searchPiece;
        private final long testTimeBudgetMillis;
        private final Random rand;

        public TestWorker(
                final int workerIdx,
                final String testName,
                final Operation op,
                final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
                final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
                final int nblocks,
                final int searchPiece,
                final TestSequenceMode mode,
                final long testTimeBudgetMillis) {
            this.workerName = "" + workerIdx + "/" + TEST_WORKERS;
            this.testName = testName;
            this.op = op;
            this.rspOp = rspOp;
            this.osetOp = osetOp;
            this.nblocks = nblocks;
            this.searchPiece = searchPiece;
            this.testTimeBudgetMillis = testTimeBudgetMillis;
            final long seedBase = RANDOM_SEED_FROM_TIME ? System.currentTimeMillis() : RANDOM_SEED_BASE_DEFAULT;
            rand = new Random(seedBase + SPLIT_SEARCH_SPACE_PIECES * workerIdx);
        }

        @Override
        public void run() {
            final long nspecs = pow(SPEC_BASE, nblocks) - 1; // avoid using the "all zeroes" (empty) case.
            final long totalSpaceSize = nspecs * nspecs;
            final long onePieceSize =
                    totalSpaceSize / SPLIT_SEARCH_SPACE_PIECES
                            + (((totalSpaceSize % SPLIT_SEARCH_SPACE_PIECES) > 0) ? 1 : 0);
            final long firstValueForSearch = onePieceSize * searchPiece;
            long lastValueForSearch = onePieceSize * (searchPiece + 1) - 1;
            if (lastValueForSearch > totalSpaceSize - 1) {
                lastValueForSearch = totalSpaceSize - 1;
            }
            final long searchSpaceSize = lastValueForSearch - firstValueForSearch + 1;
            // We will use an int value to represent a spec of whether to have for a given key:
            // 0: Key not present
            // 1: Singleton container with element zero.
            // 2: Singleton container with element one.
            // 3: Container with elements zero and one.
            // 4: Container with elements from two to BLOCK_LAST.
            // 5: Full block span.
            final int[] spec0 = new int[nblocks];
            final int[] spec1 = new int[nblocks];
            long check = 0;
            final long start = System.currentTimeMillis();
            final long deadline = (testTimeBudgetMillis == -1) ? -1 : start + testTimeBudgetMillis;
            long lastLog = start;
            long nextLog = start + LOG_PERIOD_MILLIS;
            long nextFailureCheck = start + FAILURE_CHECK_PERIOD_MILLIS;
            long lastLogChecksDone = 0;
            final String me = String.format("Worker %s %s", workerName, testName);
            log(
                    "%s: Starting, searching space (%s) for %,d blocks between %,d and %,d (%,d out of %,d combinations, %.001f%% of search space).%n",
                    me, TEST_MODE, nblocks, firstValueForSearch, lastValueForSearch, searchSpaceSize, totalSpaceSize,
                    (100.0 * searchSpaceSize) / totalSpaceSize);
            final long totalChecks = lastValueForSearch - firstValueForSearch + 1;
            while (true) {
                final long now = System.currentTimeMillis();
                if (deadline != -1 && now > deadline) {
                    break;
                }
                final long v;
                switch (TEST_MODE) {
                    case RANDOM: {
                        v = firstValueForSearch + (long) (rand.nextDouble() * searchSpaceSize);
                        break;
                    }
                    case EXHAUSTIVE: {
                        v = firstValueForSearch + check;
                        break;
                    }
                    default:
                        throw new IllegalStateException(me + " Unexpected mode");
                }
                if (TEST_MODE.equals(TestSequenceMode.EXHAUSTIVE) && v > lastValueForSearch) {
                    break;
                }
                final long spec0value = v % nspecs;
                final long spec1value = v / nspecs;
                valueToSpec(spec0value, spec0);
                valueToSpec(spec1value, spec1);
                if (!checkBinaryOp(rspOp, osetOp, spec0, spec1)) {
                    final String failed = op
                            + " spec0=" + Arrays.toString(spec0)
                            + " spec1=" + Arrays.toString(spec1)
                            + "\n"
                            + " spec0 => " + toStr(spec0)
                            + "\n"
                            + " spec1 => " + toStr(spec1)
                            + "\n";
                    log("%s: failed, %s.%n", me, failed);
                    if (failure == null) {
                        synchronized (TestWorker.class) {
                            if (failure == null) {
                                failure = failed;
                            }
                        }
                    }
                    return;
                }
                if (now >= nextFailureCheck) {
                    if (failure != null) {
                        log("%s: detected failure in another worker, stopping.%n", me);
                        return;
                    }
                    nextFailureCheck = now + FAILURE_CHECK_PERIOD_MILLIS;
                }
                if (now >= nextLog) {
                    final long checksDoneSinceLastLog = check - lastLogChecksDone;
                    final long millis = now - lastLog;
                    final String deadlineStr = (deadline == -1) ? "no" : (toTimeStr(deadline - now) + " to");
                    final double checksPerMilli = ((double) checksDoneSinceLastLog) / millis;
                    final double avgChecksPerMilli = ((double) (check + 1)) / (now - start);
                    log("%s: " +
                            "In the last %.1f seconds ran %.1f checks per second; " +
                            "%.3f%% of this worker's space covered; %s test deadline, " +
                            "%s to worker's space coverage.%n",
                            me,
                            millis / 1000.0,
                            1000.0 * checksPerMilli,
                            (100.0 * (check + 1)) / totalChecks,
                            deadlineStr,
                            toTimeStr((long) ((totalChecks - check - 1) / avgChecksPerMilli)));
                    log(
                            "%s: Last checked for v=%,d => spec0=%s, spec1=%s.%n",
                            me,
                            v,
                            Arrays.toString(spec0),
                            Arrays.toString(spec1));
                    lastLog = now;
                    nextLog = now + LOG_PERIOD_MILLIS;
                    lastLogChecksDone = check;
                }
                ++check;
            }
            log("%s: Done in %s.%n",
                    me,
                    toTimeStr(System.currentTimeMillis() - start));
        }
    }

    private static void binaryOpsHelper(
            final Operation op,
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
            final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
            final int nblocks,
            final TestSequenceMode mode,
            final long testTimeBudgetMillis) {
        final String testName = "binaryOps-" + op + "-" + mode + "-"
                + ((testTimeBudgetMillis == -1) ? "nolimit" : toTimeStr(testTimeBudgetMillis));
        final Thread[] workers = new Thread[TEST_WORKERS];
        for (int i = 0; i < TEST_WORKERS; ++i) {
            workers[i] = new Thread(
                    new TestWorker(
                            i,
                            testName,
                            op, rspOp, osetOp,
                            nblocks,
                            (mode == TestSequenceMode.RANDOM) ? 0 : (SPLIT_SEARCH_SPACE_FIRST_PIECE + i),
                            mode,
                            testTimeBudgetMillis),
                    "worker-" + i);
            log("%s: Dispatching worker %d.%n", testName, i);
            workers[i].start();
            if (TEST_WORKERS > 1 && i < TEST_WORKERS - 1) {
                // minimally stagger
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    /* ignore */
                }
            }
        }
        for (int i = 0; i < TEST_WORKERS; ++i) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                // ignore.
            }
        }
        log("%s: All workers finished.%n", testName);
        if (TestWorker.failure != null) {
            assertTrue(TestWorker.failure, TestWorker.failure == null);
        }
    }

    private static boolean checkBinaryOp(
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
            final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
            final int[] spec0,
            final int[] spec1) {
        final OrderedLongSet oset0 = fromBlockSpec(spec0);
        if (oset0 instanceof RspBitmap) {
            throw new IllegalStateException("Unexpected OrderedLongSet subtype");
        }
        final OrderedLongSet oset1 = fromBlockSpec(spec1);
        if (oset1 instanceof RspBitmap) {
            throw new IllegalStateException("Unexppected OrderedLongSet subtype");
        }
        final RspBitmap r0 = oset0.ixToRspOnNew();
        final RspBitmap r1 = oset1.ixToRspOnNew();
        final RspBitmap rspResult = rspOp.apply(r0, r1);
        final OrderedLongSet osetResult = osetOp.apply(oset0, oset1);
        return areEqual(osetResult, rspResult);
    }

    @Test
    public void testAndNotEqualsRandom() {
        if (!TEST_ANDNOT) {
            return;
        }
        final Operation op = Operation.AND_NOT_EQUALS;
        binaryOpsHelper(op, rspOps.get(op), osetOps.get(op), 8, TestSequenceMode.RANDOM,
                PER_TEST_TIME_BUDGET_MILLIS);
    }

    @Test
    public void testAndEqualsRandom() {
        if (!TEST_AND) {
            return;
        }
        final Operation op = Operation.AND_EQUALS;
        binaryOpsHelper(op, rspOps.get(op), osetOps.get(op), 8, TestSequenceMode.RANDOM,
                PER_TEST_TIME_BUDGET_MILLIS);
    }

    @Test
    public void testOrEqualsRandom() {
        if (!TEST_OR) {
            return;
        }
        final Operation op = Operation.OR_EQUALS;
        binaryOpsHelper(op, rspOps.get(op), osetOps.get(op), 8, TestSequenceMode.RANDOM,
                PER_TEST_TIME_BUDGET_MILLIS);
    }
}
