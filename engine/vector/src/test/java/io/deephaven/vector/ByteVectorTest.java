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

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteVectorTest extends TestCase {

    public void testVectorDirect() {
        final ByteVector vector = new ByteVectorDirect((byte) 10, (byte) 20, (byte) 30);
        assertEquals(3, vector.size());
        assertEquals((byte) 10, vector.get(0));
        assertEquals((byte) 20, vector.get(1));
        assertEquals((byte) 30, vector.get(2));
        assertEquals(NULL_BYTE, vector.get(3));
        assertEquals(NULL_BYTE, vector.get(-1));
        final byte[] bytes = vector.toArray();
        assertEquals((byte) 10, bytes[0]);
        assertEquals((byte) 20, bytes[1]);
        assertEquals((byte) 30, bytes[2]);
        assertEquals(3, bytes.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_BYTE, vector.subVector(0, 0).get(0));
        assertEquals(NULL_BYTE, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        final byte[] bytes3 = vector.subVector(0, 1).toArray();
        assertEquals(1, bytes3.length);
        assertEquals((byte) 10, bytes3[0]);

        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        final byte[] bytes1 = vector.subVector(1, 2).toArray();
        assertEquals(1, bytes1.length);
        assertEquals((byte) 20, bytes1[0]);
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        final byte[] bytes2 = vector.subVector(1, 3).toArray();
        assertEquals(2, bytes2.length);
        assertEquals((byte) 20, bytes2[0]);
        assertEquals((byte) 30, bytes2[1]);
        assertEquals(NULL_BYTE, vector.subVector(1, 3).get(2));
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        ByteVector vector = new ByteVectorDirect((byte) 10, (byte) 20, (byte) 30);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final byte[] result = new byte[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_BYTE : vector.get(ei);
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

                        final byte[] result = new byte[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_BYTE : vector.get(ei);
                        }

                        final byte[] result2 = new byte[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_BYTE : result[ei];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(ByteVector.type().clazz(), ByteVector.class);
    }

    private void checkSubArray(
            final ByteVector vector,
            final int start,
            final int end,
            final byte[] result) {
        final ByteVector subArray = vector.subVector(start, end);
        final byte[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private void checkDoubleSubArray(
            final ByteVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final byte[] result) {
        ByteVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        final byte[] array = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subArray.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }
}
