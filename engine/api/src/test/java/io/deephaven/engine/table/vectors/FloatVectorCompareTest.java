//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import junit.framework.TestCase;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatVectorCompareTest {
    protected FloatVector makeTestVector(final float... data) {
        return new FloatVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }

    @Test
    public void testComparisonNaNValues() {
        final float[] small = new float[] {(float) 10, NULL_FLOAT};
        final float[] medium = new float[] {(float) 10, (float) 30};
        final float[] large = new float[] {(float) 10, Float.NaN};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(small);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(medium);
        final FloatVectorDirect cvd3 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testInfinities() {
        final float[] small = new float[] {(float) 10, Float.NEGATIVE_INFINITY};
        final float[] medium = new float[] {(float) 10, (float) 30};
        final float[] large = new float[] {(float) 10, Float.POSITIVE_INFINITY};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(small);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(medium);
        final FloatVectorDirect cvd3 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testInfinitiesAndNan() {
        final float[] small = new float[] {(float) 10, Float.NEGATIVE_INFINITY};
        final float[] medium = new float[] {(float) 10, Float.POSITIVE_INFINITY};
        final float[] large = new float[] {(float) 10, Float.NaN};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(small);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(medium);
        final FloatVectorDirect cvd3 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testPositiveAndNegativeZero() {
        final float[] v1 = new float[] {(float) 10, (float) -0.0};
        final float[] v2 = new float[] {(float) 10, (float) 0.0};
        final float[] v3 = new float[] {(float) 10, 1};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(v1);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(v2);
        final FloatVectorDirect cvd3 = new FloatVectorDirect(v3);
        final FloatVector cvw0 = makeTestVector(v1);
        final FloatVector cvw1 = makeTestVector(v2);
        final FloatVector cvw3 = makeTestVector(v3);

        checkPairs2(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs2(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs2(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs2(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    /**
     * 0 < 1 < 3
     */
    private static void checkPairs(final FloatVector a0, final FloatVector a1, final FloatVector a3,
            final FloatVector b0, final FloatVector b1, final FloatVector b3) {
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

    /**
     * 0 == 1 < 2
     */
    private static void checkPairs2(final FloatVector a0, final FloatVector a1, final FloatVector a2,
            final FloatVector b0, final FloatVector b1, final FloatVector b2) {
        TestCase.assertEquals(0, a0.compareTo(b0));
        TestCase.assertEquals(0, a0.compareTo(b1));
        TestCase.assertTrue(a0.compareTo(b2) < 0);

        TestCase.assertEquals(0, a1.compareTo(b0));
        TestCase.assertEquals(0, a1.compareTo(b1));
        TestCase.assertTrue(a1.compareTo(b2) < 0);

        TestCase.assertTrue(a2.compareTo(b0) > 0);
        TestCase.assertTrue(a2.compareTo(b1) > 0);
        TestCase.assertEquals(0, a2.compareTo(b2));
    }
}
