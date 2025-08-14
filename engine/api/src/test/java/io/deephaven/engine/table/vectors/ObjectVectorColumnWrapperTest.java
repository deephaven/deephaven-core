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
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.vector.ObjectVectorTest;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;


/**
 * {@link ObjectVectorTest} implementation for {@link io.deephaven.engine.table.vectors.ObjectVectorColumnWrapper}.
 */
public class ObjectVectorColumnWrapperTest extends ObjectVectorTest {

    @Override
    protected ObjectVector<Object> makeTestVector(final Object... data) {
        return new ObjectVectorColumnWrapper<>(
                ArrayBackedColumnSource.getMemoryColumnSource(data, Object.class, null),
                RowSetFactory.flat(data.length));
    }


    /**
     * We are actually testing both the direct and wrapper versions here; so that we can get all combinations of direct
     * and a non-direct vector covered. This test exercises arrays of different sizes; but with equal elements.
     */
    @Test
    public void testComparisonSize() {
        final Object[] zeroElements = new Object[0];
        final Object[] oneElement = new Object[1];
        final Object[] threeElements = new Object[3];
        final ObjectVectorDirect cvd0 = new ObjectVectorDirect(zeroElements);
        final ObjectVectorDirect cvd1 = new ObjectVectorDirect(oneElement);
        final ObjectVectorDirect cvd3 = new ObjectVectorDirect(threeElements);
        final ObjectVector<Object> cvw0 = makeTestVector(zeroElements);
        final ObjectVector<Object> cvw1 = makeTestVector(oneElement);
        final ObjectVector<Object> cvw3 = makeTestVector(threeElements);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonValues() {
        final Object[] small = new Object[] {(Object) 10, (Object) 20};
        final Object[] medium = new Object[] {(Object) 10, (Object) 30};
        final Object[] large = new Object[] {(Object) 10, (Object) 40};
        final ObjectVectorDirect cvd0 = new ObjectVectorDirect(small);
        final ObjectVectorDirect cvd1 = new ObjectVectorDirect(medium);
        final ObjectVectorDirect cvd3 = new ObjectVectorDirect(large);
        final ObjectVector<Object> cvw0 = makeTestVector(small);
        final ObjectVector<Object> cvw1 = makeTestVector(medium);
        final ObjectVector<Object> cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    @Test
    public void testComparisonNullValues() {
        final Object[] small = new Object[] {(Object) 10, null};
        final Object[] medium = new Object[] {(Object) 10, (Object) 30};
        final Object[] large = new Object[] {(Object) 10, (Object) 40};
        final ObjectVectorDirect cvd0 = new ObjectVectorDirect(small);
        final ObjectVectorDirect cvd1 = new ObjectVectorDirect(medium);
        final ObjectVectorDirect cvd3 = new ObjectVectorDirect(large);
        final ObjectVector<Object> cvw0 = makeTestVector(small);
        final ObjectVector<Object> cvw1 = makeTestVector(medium);
        final ObjectVector<Object> cvw3 = makeTestVector(large);

        checkPairs(cvd0, cvd1, cvd3, cvd0, cvd1, cvd3);
        checkPairs(cvd0, cvd1, cvd3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvw0, cvw1, cvw3);
        checkPairs(cvw0, cvw1, cvw3, cvd0, cvd1, cvd3);
    }

    private static void checkPairs(final ObjectVector<Object> a0, final ObjectVector<Object> a1, final ObjectVector<Object> a3,
            final ObjectVector<Object> b0, final ObjectVector<Object> b1, final ObjectVector<Object> b3) {
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
