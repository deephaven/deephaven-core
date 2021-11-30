/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import junit.framework.TestCase;

import java.util.Arrays;

public class ObjectVectorTest extends TestCase {

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
        ObjectVector<Integer> vector = new ObjectVectorDirect<>(10, 20, 30);

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
}
