//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.CharVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * {@link CharVectorTest} implementation for {@link io.deephaven.engine.table.vectors.CharVectorColumnWrapper}.
 */
public class CharVectorColumnWrapperTest extends CharVectorTest {

    @Override
    protected CharVector makeTestVector(final char... data) {
        return new CharVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final char[] zeroElements = new char[0];
        final char[] oneElement = new char[1];
        final char[] threeElements = new char[3];
        final CharVectorDirect cvd0 = new CharVectorDirect(zeroElements);
        final CharVectorDirect cvd1 = new CharVectorDirect(oneElement);
        final CharVectorDirect cvd3 = new CharVectorDirect(threeElements);
        final CharVector cvw0 = makeTestVector(zeroElements);
        final CharVector cvw1 = makeTestVector(oneElement);
        final CharVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final char[] small = new char[] {(char) 10, (char) 20};
        final char[] medium = new char[] {(char) 10, (char) 30};
        final char[] large = new char[] {(char) 10, (char) 40};
        final CharVectorDirect cvd0 = new CharVectorDirect(small);
        final CharVectorDirect cvd1 = new CharVectorDirect(medium);
        final CharVectorDirect cvd3 = new CharVectorDirect(large);
        final CharVector cvw0 = makeTestVector(small);
        final CharVector cvw1 = makeTestVector(medium);
        final CharVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonNullValues() {
        final char[] small = new char[] {(char) 10, NULL_CHAR};
        final char[] medium = new char[] {(char) 10, (char) 30};
        final char[] large = new char[] {(char) 10, (char) 40};
        final CharVectorDirect cvd0 = new CharVectorDirect(small);
        final CharVectorDirect cvd1 = new CharVectorDirect(medium);
        final CharVectorDirect cvd3 = new CharVectorDirect(large);
        final CharVector cvw0 = makeTestVector(small);
        final CharVector cvw1 = makeTestVector(medium);
        final CharVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    private static void checkPairs(final CharVector a0, final CharVector a1, final CharVector a3,
            final CharVector b0, final CharVector b1, final CharVector b3) {
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
