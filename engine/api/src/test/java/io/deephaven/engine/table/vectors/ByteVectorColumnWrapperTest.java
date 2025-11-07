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
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.ByteVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * {@link ByteVectorTest} implementation for {@link io.deephaven.engine.table.vectors.ByteVectorColumnWrapper}.
 */
public class ByteVectorColumnWrapperTest extends ByteVectorTest {

    @Override
    protected ByteVector makeTestVector(final byte... data) {
        return new ByteVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final byte[] zeroElements = new byte[0];
        final byte[] oneElement = new byte[1];
        final byte[] threeElements = new byte[3];
        final ByteVectorDirect cvd0 = new ByteVectorDirect(zeroElements);
        final ByteVectorDirect cvd1 = new ByteVectorDirect(oneElement);
        final ByteVectorDirect cvd3 = new ByteVectorDirect(threeElements);
        final ByteVector cvw0 = makeTestVector(zeroElements);
        final ByteVector cvw1 = makeTestVector(oneElement);
        final ByteVector cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final byte[] small = new byte[] {(byte) 10, (byte) 20};
        final byte[] medium = new byte[] {(byte) 10, (byte) 30};
        final byte[] large = new byte[] {(byte) 10, (byte) 40};
        final ByteVectorDirect cvd0 = new ByteVectorDirect(small);
        final ByteVectorDirect cvd1 = new ByteVectorDirect(medium);
        final ByteVectorDirect cvd3 = new ByteVectorDirect(large);
        final ByteVector cvw0 = makeTestVector(small);
        final ByteVector cvw1 = makeTestVector(medium);
        final ByteVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonNullValues() {
        final byte[] small = new byte[] {(byte) 10, NULL_BYTE};
        final byte[] medium = new byte[] {(byte) 10, (byte) 30};
        final byte[] large = new byte[] {(byte) 10, (byte) 40};
        final ByteVectorDirect cvd0 = new ByteVectorDirect(small);
        final ByteVectorDirect cvd1 = new ByteVectorDirect(medium);
        final ByteVectorDirect cvd3 = new ByteVectorDirect(large);
        final ByteVector cvw0 = makeTestVector(small);
        final ByteVector cvw1 = makeTestVector(medium);
        final ByteVector cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    private static void checkPairs(final ByteVector a0, final ByteVector a1, final ByteVector a3,
            final ByteVector b0, final ByteVector b1, final ByteVector b3) {
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
