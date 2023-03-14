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

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongVectorTest extends TestCase {

    public void testVectorDirect() {
        final LongVector vector = new LongVectorDirect((long) 10, (long) 20, (long) 30);
        assertEquals(3, vector.size());
        assertEquals((long) 10, vector.get(0));
        assertEquals((long) 20, vector.get(1));
        assertEquals((long) 30, vector.get(2));
        assertEquals(NULL_LONG, vector.get(3));
        assertEquals(NULL_LONG, vector.get(-1));
        final long[] longs = vector.toArray();
        assertEquals((long) 10, longs[0]);
        assertEquals((long) 20, longs[1]);
        assertEquals((long) 30, longs[2]);
        assertEquals(3, longs.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_LONG, vector.subVector(0, 0).get(0));
        assertEquals(NULL_LONG, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        final long[] longs3 = vector.subVector(0, 1).toArray();
        assertEquals(1, longs3.length);
        assertEquals((long) 10, longs3[0]);

        assertEquals(NULL_LONG, vector.subVector(0, 1).get(1));
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        final long[] longs1 = vector.subVector(1, 2).toArray();
        assertEquals(1, longs1.length);
        assertEquals((long) 20, longs1[0]);
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(1));
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        final long[] longs2 = vector.subVector(1, 3).toArray();
        assertEquals(2, longs2.length);
        assertEquals((long) 20, longs2[0]);
        assertEquals((long) 30, longs2[1]);
        assertEquals(NULL_LONG, vector.subVector(1, 3).get(2));
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        LongVector vector = new LongVectorDirect((long) 10, (long) 20, (long) 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final long[] result = new long[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_LONG : vector.get(ei);
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

                        final long[] result = new long[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_LONG : vector.get(ei);
                        }

                        final long[] result2 = new long[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_LONG : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(LongVector.type().clazz(), LongVector.class);
    }

    private void checkSubArray(
            final LongVector vector,
            final int start,
            final int end,
            final long[] result) {
        final LongVector subArray = vector.subVector(start, end);
        final long[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final LongVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final long[] result) {
        LongVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final long[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
