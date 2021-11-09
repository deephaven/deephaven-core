/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.dbarrays;

import io.deephaven.engine.v2.dbarrays.ObjectVectorColumnWrapper;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.v2.sources.IntegerArraySource;
import io.deephaven.engine.v2.utils.RowSetFactory;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class ObjectVectorTest extends TestCase {

    public void testColumnWrapper() {
        // noinspection unchecked
        ObjectVector vector = new ObjectVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new String[] {"a", "b", "c"}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals("a", vector.get(0));
        assertEquals("b", vector.get(1));
        assertEquals("c", vector.get(2));
        assertEquals(null, vector.get(3));
        assertEquals(null, vector.get(-1));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(vector.toArray()));
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(Arrays.asList(), Arrays.asList(vector.subVector(0, 0).toArray()));
        assertEquals(null, vector.subVector(0, 0).get(0));
        assertEquals(null, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        assertEquals(Arrays.asList("a"), Arrays.asList(vector.subVector(0, 1).toArray()));
        assertEquals(null, vector.subVector(0, 1).get(1));
        assertEquals(null, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        assertEquals(Arrays.asList("b"), Arrays.asList(vector.subVector(1, 2).toArray()));
        assertEquals(null, vector.subVector(0, 1).get(1));
        assertEquals(null, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        assertEquals(Arrays.asList("b", "c"), Arrays.asList(vector.subVector(1, 3).toArray()));
        assertEquals(null, vector.subVector(1, 3).get(2));
        assertEquals(null, vector.subVector(0, 1).get(-1));

    }

    public void testDirect() {
        ObjectVectorDirect vectorDirect = new ObjectVectorDirect<>("a", "b", "c");
        assertEquals(3, vectorDirect.size());
        assertEquals("a", vectorDirect.get(0));
        assertEquals("b", vectorDirect.get(1));
        assertEquals("c", vectorDirect.get(2));
        assertEquals(null, vectorDirect.get(3));
        assertEquals(null, vectorDirect.get(-1));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(vectorDirect.toArray()));
        assertEquals(0, vectorDirect.subVector(0, 0).size());
        assertEquals(Arrays.asList(), Arrays.asList(vectorDirect.subVector(0, 0).toArray()));
        assertEquals(null, vectorDirect.subVector(0, 0).get(0));
        assertEquals(null, vectorDirect.subVector(0, 0).get(-1));

        assertEquals(1, vectorDirect.subVector(0, 1).size());
        assertEquals(Arrays.asList("a"), Arrays.asList(vectorDirect.subVector(0, 1).toArray()));
        assertEquals(null, vectorDirect.subVector(0, 1).get(1));
        assertEquals(null, vectorDirect.subVector(0, 1).get(-1));

        assertEquals(1, vectorDirect.subVector(1, 2).size());
        assertEquals(Arrays.asList("b"), Arrays.asList(vectorDirect.subVector(1, 2).toArray()));
        assertEquals(null, vectorDirect.subVector(0, 1).get(1));
        assertEquals(null, vectorDirect.subVector(0, 1).get(-1));

        assertEquals(2, vectorDirect.subVector(1, 3).size());
        assertEquals(Arrays.asList("b", "c"), Arrays.asList(vectorDirect.subVector(1, 3).toArray()));
        assertEquals(null, vectorDirect.subVector(1, 3).get(2));
        assertEquals(null, vectorDirect.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        // noinspection unchecked
        ObjectVector vector = new ObjectVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new Object[] {10, 20, 30}),
                RowSetFactory.fromRange(0, 2));

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                Object result[] = new Object[end - start];

                for (int i = start; i < end; i++) {
                    result[i - start] = (i < 0 || i >= vector.size()) ? null : vector.get(i);
                }

                checkSubArray(vector, start, end, result);
            }
        }

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                for (int start2 = -4; start2 <= 4; start2++) {
                    for (int end2 = -1; end2 <= 7; end2++) {
                        if (start > end || start2 > end2) {
                            continue;
                        }

                        Object result[] = new Object[end - start];

                        for (int i = start; i < end; i++) {
                            result[i - start] = (i < 0 || i >= vector.size()) ? null : vector.get(i);
                        }

                        Object result2[] = new Object[end2 - start2];

                        for (int i = start2; i < end2; i++) {
                            result2[i - start2] = (i < 0 || i >= result.length) ? null : result[i];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    private void checkSubArray(ObjectVector vector, int start, int end, Object result[]) {
        ObjectVector subArray = vector.subVector(start, end);
        Object array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i = 0; i < result.length; i++) {
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(ObjectVector vector, int start, int end, int start2, int end2, Object result[]) {
        ObjectVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        Object array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i = 0; i < result.length; i++) {
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    public void testSubArrayByPositions() {
        final IntegerArraySource integerArraySource = new IntegerArraySource();
        integerArraySource.ensureCapacity(6);
        for (int ii = 0; ii < 6; ++ii) {
            integerArraySource.set(ii, (ii + 1) * 10);
        }
        ObjectVector<Integer> columnVector =
                new ObjectVectorColumnWrapper<>(integerArraySource, RowSetFactory.fromRange(0, 5));
        IntVector intVectorDirect = new IntVectorDirect(10, 20, 30, 40, 50, 60);

        Random random = new Random(42);

        for (int step = 0; step < 50; ++step) {
            ArrayList<Integer> expected = new ArrayList<>();
            TLongList positions = new TLongArrayList();
            for (int ii = 0; ii < 6; ++ii) {
                if (random.nextBoolean()) {
                    expected.add(((ii + 1) * 10));
                    positions.add(ii);
                }
            }

            ObjectVector<Integer> columnResult =
                    columnVector.subVectorByPositions(positions.toArray(new long[positions.size()]));
            IntVector directResult = intVectorDirect.subVectorByPositions(positions.toArray(new long[positions.size()]));

            assertEquals(expected.size(), columnResult.size());
            assertEquals(expected.size(), directResult.size());

            for (int ii = 0; ii < expected.size(); ++ii) {
                assertEquals(expected.get(ii), columnResult.get(ii));
                assertEquals((int) expected.get(ii), directResult.get(ii));
            }
        }
    }

    /**
     * Verify that a ObjectVectorColumnWrapper can correctly invoke the 'getDirect' operation even when one of the column
     * sources is null.
     */
    public void testGetDirect() {
        ObjectVectorDirect vectorDirect = new ObjectVectorDirect<>("a", "b", "c");
        // noinspection unchecked
        ObjectVectorColumnWrapper vector = new ObjectVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new Vector[] {vectorDirect, null}),
                RowSetFactory.fromRange(0, 1));
        Vector base = vector.getDirect();
        assertEquals(2, base.intSize());
        assertTrue(ObjectVectorDirect.class.isAssignableFrom(base.getClass()));
        assertNull(((ObjectVectorDirect) base).get(1));
    }

}
