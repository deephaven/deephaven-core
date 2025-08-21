//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.util.QueryConstants;
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
        final FloatVectorDirect cvd2 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testInfinities() {
        final float[] small = new float[] {(float) 10, Float.NEGATIVE_INFINITY};
        final float[] medium = new float[] {(float) 10, (float) 30};
        final float[] large = new float[] {(float) 10, Float.POSITIVE_INFINITY};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(small);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(medium);
        final FloatVectorDirect cvd2 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testInfinitiesAndNan() {
        final float[] small = new float[] {(float) 10, Float.NEGATIVE_INFINITY};
        final float[] medium = new float[] {(float) 10, Float.POSITIVE_INFINITY};
        final float[] large = new float[] {(float) 10, Float.NaN};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(small);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(medium);
        final FloatVectorDirect cvd2 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testInfinitiesAndNull() {
        final float[] small = new float[] {(float) 10, QueryConstants.NULL_FLOAT};
        final float[] medium = new float[] {(float) 10, Float.NEGATIVE_INFINITY};
        final float[] large = new float[] {(float) 10, Float.POSITIVE_INFINITY};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(small);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(medium);
        final FloatVectorDirect cvd2 = new FloatVectorDirect(large);
        final FloatVector cvw0 = makeTestVector(small);
        final FloatVector cvw1 = makeTestVector(medium);
        final FloatVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testPositiveAndNegativeZero() {
        final float[] v1 = new float[] {(float) 10, (float) -0.0};
        final float[] v2 = new float[] {(float) 10, (float) 0.0};
        final float[] v3 = new float[] {(float) 10, 1};
        final FloatVectorDirect cvd0 = new FloatVectorDirect(v1);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(v2);
        final FloatVectorDirect cvd2 = new FloatVectorDirect(v3);
        final FloatVector cvw0 = makeTestVector(v1);
        final FloatVector cvw1 = makeTestVector(v2);
        final FloatVector cvw2 = makeTestVector(v3);

        checkPairs2(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs2(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs2(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs2(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    /**
     * 0 < 1 < 2
     */
    private static void checkPairs(final FloatVector a0, final FloatVector a1, final FloatVector a2,
            final FloatVector b0, final FloatVector b1, final FloatVector b2) {
        TestCase.assertEquals(0, a0.compareTo(b0));
        TestCase.assertTrue(a0.compareTo(b1) < 0);
        TestCase.assertTrue(a0.compareTo(b2) < 0);

        TestCase.assertTrue(a1.compareTo(b0) > 0);
        TestCase.assertEquals(0, a1.compareTo(b1));
        TestCase.assertTrue(a1.compareTo(b2) < 0);

        TestCase.assertTrue(a2.compareTo(b0) > 0);
        TestCase.assertTrue(a2.compareTo(b1) > 0);
        TestCase.assertEquals(0, a2.compareTo(b2));

        TestCase.assertEquals(a0, a0);
        TestCase.assertEquals(a0, b0);
        TestCase.assertEquals(a0.hashCode(), b0.hashCode());
        TestCase.assertFalse(a0.equals(a1));
        TestCase.assertFalse(a0.equals(a1));
        TestCase.assertFalse(a0.equals(a2));
        TestCase.assertFalse(a0.equals(b2));

        TestCase.assertFalse(a1.equals(a0));
        TestCase.assertFalse(a1.equals(b0));
        TestCase.assertEquals(a1, a1);
        TestCase.assertEquals(a1, b1);
        TestCase.assertEquals(a1.hashCode(), b1.hashCode());
        TestCase.assertFalse(a1.equals(a2));
        TestCase.assertFalse(a1.equals(b2));

        TestCase.assertFalse(a2.equals(a0));
        TestCase.assertFalse(a2.equals(b0));
        TestCase.assertFalse(a2.equals(a1));
        TestCase.assertFalse(a2.equals(b1));
        TestCase.assertEquals(a2, a2);
        TestCase.assertEquals(a2, b2);
        TestCase.assertEquals(a2.hashCode(), b2.hashCode());
    }

    /**
     * 0 == 1 < 2
     */
    private static void checkPairs2(final FloatVector a0, final FloatVector a1, final FloatVector a2,
            final FloatVector b0, final FloatVector b1, final FloatVector b2) {
        TestCase.assertEquals(0, a0.compareTo(b0));
        TestCase.assertEquals(0, a0.compareTo(b1));
        TestCase.assertTrue(a0.compareTo(b2) < 0);

        TestCase.assertEquals(a0.hashCode(), a1.hashCode());
        TestCase.assertEquals(a0.hashCode(), b0.hashCode());
        TestCase.assertEquals(a0.hashCode(), b1.hashCode());

        TestCase.assertEquals(0, a1.compareTo(b0));
        TestCase.assertEquals(0, a1.compareTo(b1));
        TestCase.assertTrue(a1.compareTo(b2) < 0);

        TestCase.assertTrue(a2.compareTo(b0) > 0);
        TestCase.assertTrue(a2.compareTo(b1) > 0);
        TestCase.assertEquals(0, a2.compareTo(b2));
    }
}
