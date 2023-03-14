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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatVectorTest extends TestCase {

    public void testVectorDirect() {
        final FloatVector vector = new FloatVectorDirect((float) 10, (float) 20, (float) 30);
        assertEquals(3, vector.size());
        assertEquals((float) 10, vector.get(0));
        assertEquals((float) 20, vector.get(1));
        assertEquals((float) 30, vector.get(2));
        assertEquals(NULL_FLOAT, vector.get(3));
        assertEquals(NULL_FLOAT, vector.get(-1));
        final float[] floats = vector.toArray();
        assertEquals((float) 10, floats[0]);
        assertEquals((float) 20, floats[1]);
        assertEquals((float) 30, floats[2]);
        assertEquals(3, floats.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_FLOAT, vector.subVector(0, 0).get(0));
        assertEquals(NULL_FLOAT, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        final float[] floats3 = vector.subVector(0, 1).toArray();
        assertEquals(1, floats3.length);
        assertEquals((float) 10, floats3[0]);

        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        final float[] floats1 = vector.subVector(1, 2).toArray();
        assertEquals(1, floats1.length);
        assertEquals((float) 20, floats1[0]);
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        final float[] floats2 = vector.subVector(1, 3).toArray();
        assertEquals(2, floats2.length);
        assertEquals((float) 20, floats2[0]);
        assertEquals((float) 30, floats2[1]);
        assertEquals(NULL_FLOAT, vector.subVector(1, 3).get(2));
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        FloatVector vector = new FloatVectorDirect((float) 10, (float) 20, (float) 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final float[] result = new float[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_FLOAT : vector.get(ei);
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

                        final float[] result = new float[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_FLOAT : vector.get(ei);
                        }

                        final float[] result2 = new float[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_FLOAT : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(FloatVector.type().clazz(), FloatVector.class);
    }

    private void checkSubArray(
            final FloatVector vector,
            final int start,
            final int end,
            final float[] result) {
        final FloatVector subArray = vector.subVector(start, end);
        final float[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final FloatVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final float[] result) {
        FloatVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final float[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
