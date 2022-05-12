package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

@Category(OutOfBandTest.class)
public class RspBitmapTimeDrivenTest {

    public static final int RANDOM_SEED = 4;
    public static final long DEFAULT_PER_TEST_TIME_BUDGET_MILLIS = 2 * 60 * 1000;
    public static final long LOG_PERIOD_MILLIS = 10 * 1000;

    // See comment about spec value meanings in caller.
    public static final int SPEC_OPTIONS = 6;

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
        return b.getTreeIndexImpl();
    }

    private static String toStr(final int[] spec) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < spec.length; ++i) {
            final long baseKey = BLOCK_SIZE * (long) i;
            sb.append(" ").append(baseKey).append(" : ");
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
                    sb.append("{2->}");
                    break;
                case 5: // Full block span.
                    sb.append("{0->}");
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

    private static final Random rand = new Random(RANDOM_SEED);

    private static void random(final int[] spec) {
        for (int i = 0; i < spec.length; ++i) {
            spec[i] = rand.nextInt(SPEC_OPTIONS);
        }
    }

    // Do a "manual" increment of spec as if it was a number represented in base SPEC_OPTIONS
    // with one digit per array position.
    private static boolean increment(final int[] spec) {
        for (int i = spec.length - 1; i >= 0; --i) {
            final int v = ++spec[i];
            if (v < SPEC_OPTIONS) {
                return true;
            }
            spec[i] = 0;
        }
        return false;
    }

    private static void checkEquals(final String msg, final OrderedLongSet oset0, final OrderedLongSet oset1) {
        assertEquals(msg, oset0.ixCardinality(), oset1.ixCardinality());
        assertTrue(msg, oset0.ixSubsetOf(oset1));
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
        AND_NOT_EQUALS, AND_EQUALS, OR_EQUALS
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

    enum TestSequenceMode {
        EXHAUSTIVE, RANDOM
    }

    private static String toTimeStr(final long millis) {
        final long totalDeciSeconds = millis / 100;
        final long totalSeconds = totalDeciSeconds / 10;
        final long deciSeconds = totalDeciSeconds % 10;
        final long minutes = totalSeconds / 60;
        final long seconds = totalSeconds % 60;
        return String.format("%dm%d.%ds", minutes, seconds, deciSeconds);
    }

    private static void binaryOpsExhaustiveHelper(
            final Operation op,
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
            final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
            final int nblocks,
            final TestSequenceMode mode,
            final long testTimeBudgetMillis) {
        final long nspecs = pow(SPEC_OPTIONS, nblocks);
        final long totalChecks = nspecs * nspecs;
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
        final long deadline = start + testTimeBudgetMillis;
        long lastLog = start;
        long nextLog = start + LOG_PERIOD_MILLIS;
        long lastLogChecksDone = 0;
        final String testName = "binaryOps-" + op + "-" + mode + "-" + toTimeStr(testTimeBudgetMillis);
        System.out.printf("%s: Starting, search space for %d blocks is %d combinations.%n", testName, nblocks,
                totalChecks);
        while (true) {
            final long now = System.currentTimeMillis();
            if (now > deadline) {
                break;
            }
            boolean finished = false;
            switch (mode) {
                case RANDOM:
                    random(spec0);
                    random(spec1);
                case EXHAUSTIVE:
                    // we always increment both in the first pass, to avoid
                    // checking against the "all empty" case.
                    final boolean more0 = increment(spec0);
                    if (!more0 || check == 0) {
                        final boolean more1 = increment(spec1);
                        if (!more1) {
                            finished = true;
                        }
                    }
                    break;
            }
            if (finished) {
                break;
            }
            if (now >= nextLog) {
                final long checksDoneSinceLastLog = check - lastLogChecksDone;
                final long millis = now - lastLog;
                System.out.printf(
                        "%s: In the last %.1f seconds ran %.1f checks per second; %.3f%% of the space covered; %s to test deadline%n",
                        testName,
                        millis / 1000.0,
                        (1000.0 * checksDoneSinceLastLog) / millis,
                        (100.0 * check) / totalChecks,
                        toTimeStr(deadline - now));
                lastLog = now;
                nextLog = now + LOG_PERIOD_MILLIS;
                lastLogChecksDone = check;
            }
            binaryOpHelper(op, rspOp, osetOp, spec0, spec1);
            ++check;
        }
        System.out.printf("%s: Done in %s.%n",
                testName,
                toTimeStr(System.currentTimeMillis() - start));
    }

    private static void binaryOpHelper(
            final Operation op,
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
            final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
            final int[] spec0,
            final int[] spec1) {
        final String msg = op
                + " && spec0==" + toStr(spec0)
                + " && spec1==" + toStr(spec1);
        final OrderedLongSet oset0 = fromBlockSpec(spec0);
        assertFalse(msg, oset0 instanceof RspBitmap);
        final OrderedLongSet oset1 = fromBlockSpec(spec1);
        assertFalse(msg, oset1 instanceof RspBitmap);
        final RspBitmap r0 = oset0.ixToRspOnNew();
        final RspBitmap r1 = oset1.ixToRspOnNew();
        final RspBitmap rspResult = rspOp.apply(r0, r1);
        final OrderedLongSet osetResult = osetOp.apply(oset0, oset1);
        checkEquals(msg, osetResult, rspResult);
    }

    @Test
    public void testBinaryOpsExhaustive() {
        for (Operation op : rspOps.keySet()) {
            binaryOpsExhaustiveHelper(op, rspOps.get(op), osetOps.get(op), 4, TestSequenceMode.EXHAUSTIVE,
                    DEFAULT_PER_TEST_TIME_BUDGET_MILLIS);
        }
    }

    @Test
    public void testAndNotEqualsRandom() {
        final Operation op = Operation.AND_NOT_EQUALS;
        binaryOpsExhaustiveHelper(op, rspOps.get(op), osetOps.get(op), 8, TestSequenceMode.RANDOM,
                DEFAULT_PER_TEST_TIME_BUDGET_MILLIS);
    }

    @Test
    public void testAndEqualsRandom() {
        final Operation op = Operation.AND_EQUALS;
        binaryOpsExhaustiveHelper(op, rspOps.get(op), osetOps.get(op), 8, TestSequenceMode.RANDOM,
                DEFAULT_PER_TEST_TIME_BUDGET_MILLIS);
    }

    @Test
    public void testOrEqualsRandom() {
        final Operation op = Operation.OR_EQUALS;
        binaryOpsExhaustiveHelper(op, rspOps.get(op), osetOps.get(op), 8, TestSequenceMode.RANDOM,
                DEFAULT_PER_TEST_TIME_BUDGET_MILLIS);
    }
}
