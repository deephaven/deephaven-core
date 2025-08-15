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
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.vector.ShortVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * {@link ShortVectorTest} implementation for {@link io.deephaven.engine.table.vectors.ShortVectorColumnWrapper}.
 */
public class ShortVectorColumnWrapperTest extends ShortVectorTest {

    @Override
    protected ShortVector makeTestVector(final short... data) {
        return new ShortVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final short[] zeroElements = new short[0];
        final short[] oneElement = new short[1];
        final short[] threeElements = new short[3];
        final ShortVectorDirect cvd0 = new ShortVectorDirect(zeroElements);
        final ShortVectorDirect cvd1 = new ShortVectorDirect(oneElement);
        final ShortVectorDirect cvd3 = new ShortVectorDirect(threeElements);
        final ShortVector cvw0 = makeTestVector(zeroElements);
        final ShortVector cvw1 = makeTestVector(oneElement);
        final ShortVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final short[] small = new short[] {(short) 10, (short) 20};
        final short[] medium = new short[] {(short) 10, (short) 30};
        final short[] large = new short[] {(short) 10, (short) 40};
        final ShortVectorDirect cvd0 = new ShortVectorDirect(small);
        final ShortVectorDirect cvd1 = new ShortVectorDirect(medium);
        final ShortVectorDirect cvd3 = new ShortVectorDirect(large);
        final ShortVector cvw0 = makeTestVector(small);
        final ShortVector cvw1 = makeTestVector(medium);
        final ShortVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonNullValues() {
        final short[] small = new short[] {(short) 10, NULL_SHORT};
        final short[] medium = new short[] {(short) 10, (short) 30};
        final short[] large = new short[] {(short) 10, (short) 40};
        final ShortVectorDirect cvd0 = new ShortVectorDirect(small);
        final ShortVectorDirect cvd1 = new ShortVectorDirect(medium);
        final ShortVectorDirect cvd3 = new ShortVectorDirect(large);
        final ShortVector cvw0 = makeTestVector(small);
        final ShortVector cvw1 = makeTestVector(medium);
        final ShortVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    private static void checkPairs(final ShortVector a0, final ShortVector a1, final ShortVector a3,
            final ShortVector b0, final ShortVector b1, final ShortVector b3) {
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
