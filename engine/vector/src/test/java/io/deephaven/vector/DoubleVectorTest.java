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

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleVectorTest extends TestCase {

    public void testVectorDirect() {
        final DoubleVector vector = new DoubleVectorDirect((double) 10, (double) 20, (double) 30);
        assertEquals(3, vector.size());
        assertEquals((double) 10, vector.get(0));
        assertEquals((double) 20, vector.get(1));
        assertEquals((double) 30, vector.get(2));
        assertEquals(NULL_DOUBLE, vector.get(3));
        assertEquals(NULL_DOUBLE, vector.get(-1));
        final double[] doubles = vector.toArray();
        assertEquals((double) 10, doubles[0]);
        assertEquals((double) 20, doubles[1]);
        assertEquals((double) 30, doubles[2]);
        assertEquals(3, doubles.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_DOUBLE, vector.subVector(0, 0).get(0));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        final double[] doubles3 = vector.subVector(0, 1).toArray();
        assertEquals(1, doubles3.length);
        assertEquals((double) 10, doubles3[0]);

        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        final double[] doubles1 = vector.subVector(1, 2).toArray();
        assertEquals(1, doubles1.length);
        assertEquals((double) 20, doubles1[0]);
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        final double[] doubles2 = vector.subVector(1, 3).toArray();
        assertEquals(2, doubles2.length);
        assertEquals((double) 20, doubles2[0]);
        assertEquals((double) 30, doubles2[1]);
        assertEquals(NULL_DOUBLE, vector.subVector(1, 3).get(2));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        DoubleVector vector = new DoubleVectorDirect((double) 10, (double) 20, (double) 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final double[] result = new double[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_DOUBLE : vector.get(ei);
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

                        final double[] result = new double[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_DOUBLE : vector.get(ei);
                        }

                        final double[] result2 = new double[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_DOUBLE : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DoubleVector.type().clazz(), DoubleVector.class);
    }

    private void checkSubArray(
            final DoubleVector vector,
            final int start,
            final int end,
            final double[] result) {
        final DoubleVector subArray = vector.subVector(start, end);
        final double[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final DoubleVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final double[] result) {
        DoubleVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final double[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
