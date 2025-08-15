//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorColumnWrapperTest and run "./gradlew replicateVectorColumnWrappers" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.IntVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link IntVectorTest} implementation for {@link io.deephaven.engine.table.vectors.IntVectorColumnWrapper}.
 */
public class IntVectorColumnWrapperTest extends IntVectorTest {

    @Override
    protected IntVector makeTestVector(final int... data) {
        return new IntVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final int[] zeroElements = new int[0];
        final int[] oneElement = new int[1];
        final int[] threeElements = new int[3];
        final IntVectorDirect cvd0 = new IntVectorDirect(zeroElements);
        final IntVectorDirect cvd1 = new IntVectorDirect(oneElement);
        final IntVectorDirect cvd3 = new IntVectorDirect(threeElements);
        final IntVector cvw0 = makeTestVector(zeroElements);
        final IntVector cvw1 = makeTestVector(oneElement);
        final IntVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final int[] small = new int[] {(int) 10, (int) 20};
        final int[] medium = new int[] {(int) 10, (int) 30};
        final int[] large = new int[] {(int) 10, (int) 40};
        final IntVectorDirect cvd0 = new IntVectorDirect(small);
        final IntVectorDirect cvd1 = new IntVectorDirect(medium);
        final IntVectorDirect cvd3 = new IntVectorDirect(large);
        final IntVector cvw0 = makeTestVector(small);
        final IntVector cvw1 = makeTestVector(medium);
        final IntVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonNullValues() {
        final int[] small = new int[] {(int) 10, NULL_INT};
        final int[] medium = new int[] {(int) 10, (int) 30};
        final int[] large = new int[] {(int) 10, (int) 40};
        final IntVectorDirect cvd0 = new IntVectorDirect(small);
        final IntVectorDirect cvd1 = new IntVectorDirect(medium);
        final IntVectorDirect cvd3 = new IntVectorDirect(large);
        final IntVector cvw0 = makeTestVector(small);
        final IntVector cvw1 = makeTestVector(medium);
        final IntVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    private static void checkPairs(final IntVector a0, final IntVector a1, final IntVector a3,
            final IntVector b0, final IntVector b1, final IntVector b3) {
        TestCase.assertEquals(0, a0.compareTo(b0));
        TestCase.assertTrue(a0.compareTo(b1) < 0);
        TestCase.assertTrue(a0.compareTo(b3) < 0);

        TestCase.assertTrue(a1.compareTo(b0) > 0);
        TestCase.assertEquals(0, a1.compareTo(b1));
        TestCase.assertTrue(a1.compareTo(b3) < 0);

        TestCase.assertTrue(a3.compareTo(b0) > 0);
        TestCase.assertTrue(a3.compareTo(b1) > 0);
        TestCase.assertEquals(0, a3.compareTo(b3));
    }
}
