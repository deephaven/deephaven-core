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
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.FloatVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * {@link FloatVectorTest} implementation for {@link io.deephaven.engine.table.vectors.FloatVectorColumnWrapper}.
 */
public class FloatVectorColumnWrapperTest extends FloatVectorTest {

    @Override
    protected FloatVector makeTestVector(final float... data) {
        return new FloatVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final float[] zeroElements = new float[0];
        final float[] oneElement = new float[1];
        final float[] threeElements = new float[3];
        final FloatVectorDirect cvd0 = new FloatVectorDirect(zeroElements);
        final FloatVectorDirect cvd1 = new FloatVectorDirect(oneElement);
        final FloatVectorDirect cvd3 = new FloatVectorDirect(threeElements);
        final FloatVector cvw0 = makeTestVector(zeroElements);
        final FloatVector cvw1 = makeTestVector(oneElement);
        final FloatVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final float[] small = new float[] {(float) 10, (float) 20};
        final float[] medium = new float[] {(float) 10, (float) 30};
        final float[] large = new float[] {(float) 10, (float) 40};
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
    public void testComparisonNullValues() {
        final float[] small = new float[] {(float) 10, NULL_FLOAT};
        final float[] medium = new float[] {(float) 10, (float) 30};
        final float[] large = new float[] {(float) 10, (float) 40};
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
}
