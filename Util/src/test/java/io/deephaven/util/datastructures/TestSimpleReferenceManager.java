package io.deephaven.util.datastructures;

import io.deephaven.base.reference.SimpleReference;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link SimpleReferenceManager}.
 */
public class TestSimpleReferenceManager {

    @Test
    public void testConcurrent() {
        doTest(true);
    }

    @Test
    public void testSerial() {
        doTest(false);
    }

    private static final class IntRef extends MutableInt implements SimpleReference<MutableInt> {

        private boolean cleared;

        public IntRef(final int value) {
            super(value);
        }

        @Override
        public MutableInt get() {
            return cleared ? null : this;
        }

        @Override
        public void clear() {
            cleared = true;
        }
    }

    @SuppressWarnings({"NumberEquality", "PointlessArithmeticExpression"})
    private void doTest(final boolean concurrent) {
        final SimpleReferenceManager<MutableInt, SimpleReference<MutableInt>> SUT =
            new SimpleReferenceManager<>((final MutableInt item) -> ((IntRef) item), concurrent);
        final IntRef[] items =
            IntStream.range(0, 1000).mapToObj(IntRef::new).toArray(IntRef[]::new);

        Arrays.stream(items, 0, 500).forEach(SUT::add);

        int expectedSum = 500 * (499 + 0) / 2;
        testSumExpectations(SUT, expectedSum);

        Arrays.stream(items, 0, 500).forEach((final IntRef item) -> TestCase.assertSame(item,
            SUT.getFirstItem((final MutableInt other) -> item == other)));
        Arrays.stream(items, 0, 500).forEach((final IntRef item) -> TestCase.assertSame(item,
            SUT.getFirstReference((final MutableInt other) -> item == other)));

        items[200].clear();
        expectedSum -= 200;
        TestCase.assertSame(items[199],
            SUT.getFirstItem((final MutableInt other) -> items[199] == other));
        TestCase.assertNull(SUT.getFirstItem((final MutableInt other) -> items[200] == other));
        TestCase.assertSame(items[201],
            SUT.getFirstItem((final MutableInt other) -> items[201] == other));
        testSumExpectations(SUT, expectedSum);

        items[300].clear();
        expectedSum -= 300;
        TestCase.assertSame(items[299],
            SUT.getFirstReference((final MutableInt other) -> items[299] == other));
        TestCase.assertNull(SUT.getFirstReference((final MutableInt other) -> items[300] == other));
        TestCase.assertSame(items[301],
            SUT.getFirstReference((final MutableInt other) -> items[301] == other));
        testSumExpectations(SUT, expectedSum);

        items[400].clear();
        expectedSum -= 400;
        testSumExpectations(SUT, expectedSum);

        Arrays.stream(items, 500, 1000).forEach(SUT::add);
        expectedSum += 500 * (999 + 500) / 2;
        testSumExpectations(SUT, expectedSum);

        SUT.removeAll(Arrays.asList(Arrays.copyOfRange(items, 600, 700)));
        Arrays.stream(items, 700, 800).forEach(IntRef::clear);
        expectedSum -= 200 * (799 + 600) / 2;
        testSumExpectations(SUT, expectedSum);

        Arrays.stream(items, 0, 100).forEach(IntRef::clear);
        SUT.remove(items[0]);
        expectedSum -= 100 * (99 + 0) / 2;
        testSumExpectations(SUT, expectedSum);
    }

    private void testSumExpectations(
        @NotNull final SimpleReferenceManager<MutableInt, SimpleReference<MutableInt>> SUT,
        final int expectedSum) {
        final MutableInt sum = new MutableInt();
        SUT.forEach((final SimpleReference<MutableInt> ref, final MutableInt item) -> {
            TestCase.assertSame(ref, item);
            sum.add(item);
        });
        TestCase.assertEquals(expectedSum, sum.intValue());
    }
}
