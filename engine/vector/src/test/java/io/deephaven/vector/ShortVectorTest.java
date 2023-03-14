/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortVectorTest extends TestCase {

    public void testVectorDirect() {
        final ShortVector vector = new ShortVectorDirect((short) 10, (short) 20, (short) 30);
        assertEquals(3, vector.size());
        assertEquals((short) 10, vector.get(0));
        assertEquals((short) 20, vector.get(1));
        assertEquals((short) 30, vector.get(2));
        assertEquals(NULL_SHORT, vector.get(3));
        assertEquals(NULL_SHORT, vector.get(-1));
        final short[] shorts = vector.toArray();
        assertEquals((short) 10, shorts[0]);
        assertEquals((short) 20, shorts[1]);
        assertEquals((short) 30, shorts[2]);
        assertEquals(3, shorts.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_SHORT, vector.subVector(0, 0).get(0));
        assertEquals(NULL_SHORT, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        final short[] shorts3 = vector.subVector(0, 1).toArray();
        assertEquals(1, shorts3.length);
        assertEquals((short) 10, shorts3[0]);

        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        final short[] shorts1 = vector.subVector(1, 2).toArray();
        assertEquals(1, shorts1.length);
        assertEquals((short) 20, shorts1[0]);
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        final short[] shorts2 = vector.subVector(1, 3).toArray();
        assertEquals(2, shorts2.length);
        assertEquals((short) 20, shorts2[0]);
        assertEquals((short) 30, shorts2[1]);
        assertEquals(NULL_SHORT, vector.subVector(1, 3).get(2));
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        ShortVector vector = new ShortVectorDirect((short) 10, (short) 20, (short) 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final short[] result = new short[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_SHORT : vector.get(ei);
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

                        final short[] result = new short[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_SHORT : vector.get(ei);
                        }

                        final short[] result2 = new short[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_SHORT : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(ShortVector.type().clazz(), ShortVector.class);
    }

    private void checkSubArray(
            final ShortVector vector,
            final int start,
            final int end,
            final short[] result) {
        final ShortVector subArray = vector.subVector(start, end);
        final short[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final ShortVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final short[] result) {
        ShortVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final short[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
