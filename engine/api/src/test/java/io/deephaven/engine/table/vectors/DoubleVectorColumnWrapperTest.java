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
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.DoubleVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * {@link DoubleVectorTest} implementation for {@link io.deephaven.engine.table.vectors.DoubleVectorColumnWrapper}.
 */
public class DoubleVectorColumnWrapperTest extends DoubleVectorTest {

    @Override
    protected DoubleVector makeTestVector(final double... data) {
        return new DoubleVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final double[] zeroElements = new double[0];
        final double[] oneElement = new double[1];
        final double[] threeElements = new double[3];
        final DoubleVectorDirect cvd0 = new DoubleVectorDirect(zeroElements);
        final DoubleVectorDirect cvd1 = new DoubleVectorDirect(oneElement);
        final DoubleVectorDirect cvd3 = new DoubleVectorDirect(threeElements);
        final DoubleVector cvw0 = makeTestVector(zeroElements);
        final DoubleVector cvw1 = makeTestVector(oneElement);
        final DoubleVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final double[] small = new double[] {(double) 10, (double) 20};
        final double[] medium = new double[] {(double) 10, (double) 30};
        final double[] large = new double[] {(double) 10, (double) 40};
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
    public void testComparisonNullValues() {
        final double[] small = new double[] {(double) 10, NULL_DOUBLE};
        final double[] medium = new double[] {(double) 10, (double) 30};
        final double[] large = new double[] {(double) 10, (double) 40};
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
}
