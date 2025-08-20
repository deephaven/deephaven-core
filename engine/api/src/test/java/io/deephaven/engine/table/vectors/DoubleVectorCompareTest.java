//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatVectorCompareTest and run "./gradlew replicateVectorColumnWrappers" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import junit.framework.TestCase;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleVectorCompareTest {
    protected DoubleVector makeTestVector(final double... data) {
        return new DoubleVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }

    @Test
    public void testComparisonNaNValues() {
        final double[] small = new double[] {(double) 10, NULL_DOUBLE};
        final double[] medium = new double[] {(double) 10, (double) 30};
        final double[] large = new double[] {(double) 10, Double.NaN};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(small);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(medium);
        final DoubleVectorDirect cvd2 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testInfinities() {
        final double[] small = new double[] {(double) 10, Double.NEGATIVE_INFINITY};
        final double[] medium = new double[] {(double) 10, (double) 30};
        final double[] large = new double[] {(double) 10, Double.POSITIVE_INFINITY};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(small);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(medium);
        final DoubleVectorDirect cvd2 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testInfinitiesAndNan() {
        final double[] small = new double[] {(double) 10, Double.NEGATIVE_INFINITY};
        final double[] medium = new double[] {(double) 10, Double.POSITIVE_INFINITY};
        final double[] large = new double[] {(double) 10, Double.NaN};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(small);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(medium);
        final DoubleVectorDirect cvd2 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testInfinitiesAndNull() {
        final double[] small = new double[] {(double) 10, QueryConstants.NULL_DOUBLE};
        final double[] medium = new double[] {(double) 10, Double.NEGATIVE_INFINITY};
        final double[] large = new double[] {(double) 10, Double.POSITIVE_INFINITY};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(small);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(medium);
        final DoubleVectorDirect cvd2 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw2 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    @Test
    public void testPositiveAndNegativeZero() {
        final double[] v1 = new double[] {(double) 10, (double) -0.0};
        final double[] v2 = new double[] {(double) 10, (double) 0.0};
        final double[] v3 = new double[] {(double) 10, 1};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(v1);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(v2);
        final DoubleVectorDirect cvd2 = new DoubleVectorDirect(v3);
        final DoubleVector cvw0 = makeTestVector(v1);
        final DoubleVector cvw1 = makeTestVector(v2);
        final DoubleVector cvw2 = makeTestVector(v3);

        checkPairs2(cvd0, cvd1, cvd2, cvd0, cvd1, cvd2);
        checkPairs2(cvd0, cvd1, cvd2, cvw0, cvw1, cvw2);
        checkPairs2(cvw0, cvw1, cvw2, cvw0, cvw1, cvw2);
        checkPairs2(cvw0, cvw1, cvw2, cvd0, cvd1, cvd2);
    }

    /**
     * 0 < 1 < 2
     */
    private static void checkPairs(final DoubleVector a0, final DoubleVector a1, final DoubleVector a2,
            final DoubleVector b0, final DoubleVector b1, final DoubleVector b2) {
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
    private static void checkPairs2(final DoubleVector a0, final DoubleVector a1, final DoubleVector a2,
            final DoubleVector b0, final DoubleVector b1, final DoubleVector b2) {
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
