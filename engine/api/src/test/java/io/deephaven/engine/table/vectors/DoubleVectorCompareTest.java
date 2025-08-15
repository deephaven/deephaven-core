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
import io.deephaven.util.compare.DoubleComparisons;
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
        final DoubleVectorDirect cvd3 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testInfinities() {
        final double[] small = new double[] {(double) 10, Double.NEGATIVE_INFINITY};
        final double[] medium = new double[] {(double) 10, (double) 30};
        final double[] large = new double[] {(double) 10, Double.POSITIVE_INFINITY};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(small);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(medium);
        final DoubleVectorDirect cvd3 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testInfinitiesAndNan() {
        final double[] small = new double[] {(double) 10, Double.NEGATIVE_INFINITY};
        final double[] medium = new double[] {(double) 10, Double.POSITIVE_INFINITY};
        final double[] large = new double[] {(double) 10, Double.NaN};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(small);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(medium);
        final DoubleVectorDirect cvd3 = new DoubleVectorDirect(large);
        final DoubleVector cvw0 = makeTestVector(small);
        final DoubleVector cvw1 = makeTestVector(medium);
        final DoubleVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testPositiveAndNegativeZero() {
        final double[] v1 = new double[] {(double) 10, (double)-0.0};
        final double[] v2 = new double[] {(double) 10, (double)0.0};
        final double[] v3 = new double[] {(double) 10, 1};
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(v1);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(v2);
        final DoubleVectorDirect cvd3 = new DoubleVectorDirect(v3);
        final DoubleVector cvw0 = makeTestVector(v1);
        final DoubleVector cvw1 = makeTestVector(v2);
        final DoubleVector cvw3 = makeTestVector(v3);

        checkPairs2(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs2(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs2(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs2(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    /**
     * 0 < 1 < 3
     */
    private static void checkPairs(final DoubleVector a0, final DoubleVector a1, final DoubleVector a3,
            final DoubleVector b0, final DoubleVector b1, final DoubleVector b3) {
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
    private static void checkPairs2(final DoubleVector a0, final DoubleVector a1, final DoubleVector a2,
            final DoubleVector b0, final DoubleVector b1, final DoubleVector b2) {
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
