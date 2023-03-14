/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import junit.framework.TestCase;

import java.util.List;

public class ObjectVectorTest extends TestCase {

    public void testDirect() {
        ObjectVectorDirect<?> vectorDirect = new ObjectVectorDirect<>("a", "b", "c");
        assertEquals(3, vectorDirect.size());
        assertEquals("a", vectorDirect.get(0));
        assertEquals("b", vectorDirect.get(1));
        assertEquals("c", vectorDirect.get(2));
        assertNull(vectorDirect.get(3));
        assertNull(vectorDirect.get(-1));
        assertEquals(List.of("a", "b", "c"), List.of(vectorDirect.toArray()));
        assertEquals(0, vectorDirect.subVector(0, 0).size());
        assertEquals(List.of(), List.of(vectorDirect.subVector(0, 0).toArray()));
        assertNull(vectorDirect.subVector(0, 0).get(0));
        assertNull(vectorDirect.subVector(0, 0).get(-1));

        assertEquals(1, vectorDirect.subVector(0, 1).size());
        assertEquals(List.of("a"), List.of(vectorDirect.subVector(0, 1).toArray()));
        assertNull(vectorDirect.subVector(0, 1).get(1));
        assertNull(vectorDirect.subVector(0, 1).get(-1));

        assertEquals(1, vectorDirect.subVector(1, 2).size());
        assertEquals(List.of("b"), List.of(vectorDirect.subVector(1, 2).toArray()));
        assertNull(vectorDirect.subVector(0, 1).get(1));
        assertNull(vectorDirect.subVector(0, 1).get(-1));

        assertEquals(2, vectorDirect.subVector(1, 3).size());
        assertEquals(List.of("b", "c"), List.of(vectorDirect.subVector(1, 3).toArray()));
        assertNull(vectorDirect.subVector(1, 3).get(2));
        assertNull(vectorDirect.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        ObjectVector<Integer> vector = new ObjectVectorDirect<>(10, 20, 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final Object[] result = new Object[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? null : vector.get(ei);
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

                        final Object[] result = new Object[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? null : vector.get(ei);
                        }

                        final Object[] result2 = new Object[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? null : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    private void checkSubArray(
            final ObjectVector<?> vector,
            final int start,
            final int end,
            final Object[] result) {
        final ObjectVector<?> subArray = vector.subVector(start, end);
        final Object[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final ObjectVector<?> vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final Object[] result) {
        ObjectVector<?> subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final Object[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
