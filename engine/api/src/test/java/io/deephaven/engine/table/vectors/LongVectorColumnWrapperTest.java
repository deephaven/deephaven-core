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
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.LongVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link LongVectorTest} implementation for {@link io.deephaven.engine.table.vectors.LongVectorColumnWrapper}.
 */
public class LongVectorColumnWrapperTest extends LongVectorTest {

    @Override
    protected LongVector makeTestVector(final long... data) {
        return new LongVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final long[] zeroElements = new long[0];
        final long[] oneElement = new long[1];
        final long[] threeElements = new long[3];
        final LongVectorDirect cvd0 = new LongVectorDirect(zeroElements);
        final LongVectorDirect cvd1 = new LongVectorDirect(oneElement);
        final LongVectorDirect cvd3 = new LongVectorDirect(threeElements);
        final LongVector cvw0 = makeTestVector(zeroElements);
        final LongVector cvw1 = makeTestVector(oneElement);
        final LongVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final long[] small = new long[] {(long) 10, (long) 20};
        final long[] medium = new long[] {(long) 10, (long) 30};
        final long[] large = new long[] {(long) 10, (long) 40};
        final LongVectorDirect cvd0 = new LongVectorDirect(small);
        final LongVectorDirect cvd1 = new LongVectorDirect(medium);
        final LongVectorDirect cvd3 = new LongVectorDirect(large);
        final LongVector cvw0 = makeTestVector(small);
        final LongVector cvw1 = makeTestVector(medium);
        final LongVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonNullValues() {
        final long[] small = new long[] {(long) 10, NULL_LONG};
        final long[] medium = new long[] {(long) 10, (long) 30};
        final long[] large = new long[] {(long) 10, (long) 40};
        final LongVectorDirect cvd0 = new LongVectorDirect(small);
        final LongVectorDirect cvd1 = new LongVectorDirect(medium);
        final LongVectorDirect cvd3 = new LongVectorDirect(large);
        final LongVector cvw0 = makeTestVector(small);
        final LongVector cvw1 = makeTestVector(medium);
        final LongVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    private static void checkPairs(final LongVector a0, final LongVector a1, final LongVector a3,
            final LongVector b0, final LongVector b1, final LongVector b3) {
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
