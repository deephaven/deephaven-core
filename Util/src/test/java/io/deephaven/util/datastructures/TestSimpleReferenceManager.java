//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import io.deephaven.base.reference.HardSimpleReference;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.util.mutable.MutableInt;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
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

    @SuppressWarnings({"NumberEquality", "PointlessArithmeticExpression"})
    private void doTest(final boolean concurrent) {
        // noinspection unchecked
        final SimpleReference<Integer>[] refs =
                IntStream.range(0, 1000).mapToObj(HardSimpleReference::new).toArray(SimpleReference[]::new);
        final SimpleReferenceManager<Integer, SimpleReference<Integer>> SUT =
                new SimpleReferenceManager<>(val -> refs[val], concurrent);

        Arrays.stream(refs, 0, 500).map(SimpleReference::get).forEach(SUT::add);

        int expectedSum = 500 * (499 + 0) / 2;
        testSumExpectations(SUT, expectedSum);

        Arrays.stream(refs, 0, 500).forEach(ref -> TestCase.assertSame(ref.get(),
                SUT.getFirstItem((final Integer other) -> ref.get() == other)));
        Arrays.stream(refs, 0, 500).forEach(ref -> TestCase.assertSame(ref,
                SUT.getFirstReference((final Integer other) -> ref.get() == other)));

        refs[200].clear();
        expectedSum -= 200;
        TestCase.assertSame(refs[199].get(), SUT.getFirstItem((final Integer other) -> refs[199].get() == other));
        TestCase.assertNull(SUT.getFirstItem((final Integer other) -> refs[200].get() == other));
        TestCase.assertSame(refs[201].get(), SUT.getFirstItem((final Integer other) -> refs[201].get() == other));
        testSumExpectations(SUT, expectedSum);

        refs[300].clear();
        expectedSum -= 300;
        TestCase.assertSame(refs[299], SUT.getFirstReference((final Integer other) -> refs[299].get() == other));
        TestCase.assertNull(SUT.getFirstReference((final Integer other) -> refs[300].get() == other));
        TestCase.assertSame(refs[301], SUT.getFirstReference((final Integer other) -> refs[301].get() == other));
        testSumExpectations(SUT, expectedSum);

        refs[400].clear();
        expectedSum -= 400;
        testSumExpectations(SUT, expectedSum);

        Arrays.stream(refs, 500, 1000).map(SimpleReference::get).forEach(SUT::add);
        expectedSum += 500 * (999 + 500) / 2;
        testSumExpectations(SUT, expectedSum);

        SUT.removeAll(Arrays.stream(Arrays.copyOfRange(refs, 600, 700)).map(SimpleReference::get)
                .collect(Collectors.toList()));
        Arrays.stream(refs, 700, 800).forEach(SimpleReference::clear);
        expectedSum -= 200 * (799 + 600) / 2;
        testSumExpectations(SUT, expectedSum);

        Arrays.stream(refs, 0, 100).forEach(SimpleReference::clear);
        SUT.remove(refs[0].get());
        expectedSum -= 100 * (99 + 0) / 2;
        testSumExpectations(SUT, expectedSum);
    }

    private void testSumExpectations(
            @NotNull final SimpleReferenceManager<Integer, ? extends SimpleReference<Integer>> SUT,
            final int expectedSum) {
        final MutableInt sum = new MutableInt();
        SUT.forEach((ref, item) -> {
            TestCase.assertSame(ref.get(), item);
            sum.add(item);
        });
        TestCase.assertEquals(expectedSum, sum.get());
    }
}
