package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

@Category(OutOfBandTest.class)
public class RspBitmapTimeDrivenTest {

    public static final long DEFAULT_PER_TEST_TIME_BUDGET_MILLIS = 3 * 60 * 1000;
    public static final long LOG_PERIOD_MILLIS = 10 * 1000;

    // See comment about spec value meanings in caller.
    public static final int SPEC_OPTIONS = 6;
    private static OrderedLongSet fromBlockSpec(final int[] spec) {
        final OrderedLongSet.BuilderSequential b = new OrderedLongSetBuilderSequential();
        for (int i = 0; i < spec.length; ++i) {
            final long baseKey = 65536L * i;
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
                    throw new IllegalStateException("Unespected spec");
            }
        }
        return b.getTreeIndexImpl();
    }

    private static String toStr(final int[] spec) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < spec.length; ++i) {
            final long baseKey = 65536L * i;
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
                    throw new IllegalStateException("Unespected spec");
            }
            if (i < spec.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

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

    private static void checkEquals(final String msg, OrderedLongSet oset0, OrderedLongSet oset1) {
        assertEquals(msg, oset0.ixCardinality(), oset1.ixCardinality());
        assertTrue(msg, oset0.ixSubsetOf(oset1));
    }

    private static long pow(int a, int b) {
        long r = 1;
        while (b > 0) {
            r *= a;
            --b;
        }
        return r;
    }

    @Test
    public void testBinaryOpsExhaustive() {
        final Map<String, BiFunction<RspBitmap, RspBitmap, RspBitmap>> rspOps = new HashMap<>();
        rspOps.put("andNotEquals", RspBitmap::andNotEquals);
        rspOps.put("andEquals", RspBitmap::andEquals);
        rspOps.put("orEquals", RspBitmap::orEquals);
        final Map<String, BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet>> osetOps = new HashMap<>();
        osetOps.put("andNotEquals", (x, y) -> x.ixCowRef().ixRemove(y));
        osetOps.put("andEquals", (x, y) -> x.ixCowRef().ixRetain(y));
        osetOps.put("orEquals", (x, y) -> x.ixCowRef().ixInsert(y));
        for (String opName : rspOps.keySet()) {
            binaryOpsExhaustiveHelper(opName, rspOps.get(opName), osetOps.get(opName), 4, TestSequenceMode.EXHAUSTIVE);
        }
    }

    enum TestSequenceMode {
        EXHAUSTIVE,
        RANDOM
    }

    private void binaryOpsExhaustiveHelper(
            final String opName,
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
            final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
            final int nblocks,
            final TestSequenceMode mode
            ) {
        final long nspecs = pow(SPEC_OPTIONS, nblocks);
        final long totalChecks = nspecs * nspecs;
        // We will use an int value to represent a spec of whether to have for a given key:
        // 0: Key not present
        // 1: Singleton container with element zero.
        // 2: Singleton container with element one.
        // 3: Container with elements zero and one.
        // 4: Container with elements from two to 65535.
        // 5: Full block span.
        final int[] spec0 = new int[nblocks];
        final int[] spec1 = new int[nblocks];
        // avoid testing against the fully empty case.
        increment(spec0);
        increment(spec1);
        long check = 0;
        final long start = System.currentTimeMillis();
        long lastLog = start;
        long nextLog = start + LOG_PERIOD_MILLIS;
        long lastLogChecksDone = 0;
        final String testName = "binaryOpsExhaustive-" + opName;
        do {
            do {
                final long now = System.currentTimeMillis();
                if (now >= nextLog) {
                    final long checksDoneSinceLastLog = check - lastLogChecksDone;
                    final long millis = now - lastLog;
                    System.out.printf("%s: In the last %.1f seconds ran %.1f checks per second; %d checks remaining.%n",
                            testName,
                            millis / 1000.0,
                            (1000.0 * checksDoneSinceLastLog) / millis,
                            totalChecks - check);
                    lastLog = now;
                    nextLog = now + LOG_PERIOD_MILLIS;
                    lastLogChecksDone = check;
                }
                binaryOpHelper(opName, rspOp, osetOp, spec0, spec1);
                ++check;
            } while (increment(spec0));
        } while (increment(spec1));
        System.out.printf("%s: Done in %.1f seconds.%n",
                testName,
                (System.currentTimeMillis() - start) / 1000.0);
    }

    private void binaryOpHelper(
            final String opName,
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> rspOp,
            final BiFunction<OrderedLongSet, OrderedLongSet, OrderedLongSet> osetOp,
            final int[] spec0,
            final int[] spec1) {
        final String msg = opName
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
}
