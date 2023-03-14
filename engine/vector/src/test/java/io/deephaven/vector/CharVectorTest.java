/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class CharVectorTest extends TestCase {

    public void testVectorDirect() {
        final CharVector vector = new CharVectorDirect((char) 10, (char) 20, (char) 30);
        assertEquals(3, vector.size());
        assertEquals((char) 10, vector.get(0));
        assertEquals((char) 20, vector.get(1));
        assertEquals((char) 30, vector.get(2));
        assertEquals(NULL_CHAR, vector.get(3));
        assertEquals(NULL_CHAR, vector.get(-1));
        final char[] chars = vector.toArray();
        assertEquals((char) 10, chars[0]);
        assertEquals((char) 20, chars[1]);
        assertEquals((char) 30, chars[2]);
        assertEquals(3, chars.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_CHAR, vector.subVector(0, 0).get(0));
        assertEquals(NULL_CHAR, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        final char[] chars3 = vector.subVector(0, 1).toArray();
        assertEquals(1, chars3.length);
        assertEquals((char) 10, chars3[0]);

        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(1));
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        final char[] chars1 = vector.subVector(1, 2).toArray();
        assertEquals(1, chars1.length);
        assertEquals((char) 20, chars1[0]);
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(1));
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        final char[] chars2 = vector.subVector(1, 3).toArray();
        assertEquals(2, chars2.length);
        assertEquals((char) 20, chars2[0]);
        assertEquals((char) 30, chars2[1]);
        assertEquals(NULL_CHAR, vector.subVector(1, 3).get(2));
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        CharVector vector = new CharVectorDirect((char) 10, (char) 20, (char) 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final char[] result = new char[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_CHAR : vector.get(ei);
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

                        final char[] result = new char[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_CHAR : vector.get(ei);
                        }

                        final char[] result2 = new char[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_CHAR : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(CharVector.type().clazz(), CharVector.class);
    }

    private void checkSubArray(
            final CharVector vector,
            final int start,
            final int end,
            final char[] result) {
        final CharVector subArray = vector.subVector(start, end);
        final char[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final CharVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final char[] result) {
        CharVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final char[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
