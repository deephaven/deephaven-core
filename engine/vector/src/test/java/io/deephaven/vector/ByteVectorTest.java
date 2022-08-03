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
        ByteVector vector = new ByteVectorDirect((byte)10, (byte)20, (byte)30);
        assertEquals(3, vector.size());
        assertEquals((byte)10, vector.get(0));
        assertEquals((byte)20, vector.get(1));
        assertEquals((byte)30, vector.get(2));
        assertEquals(NULL_BYTE,vector.get(3));
        assertEquals(NULL_BYTE,vector.get(-1));
        byte[] bytes = vector.toArray();
        assertEquals((byte)10, bytes[0]);
        assertEquals((byte)20, bytes[1]);
        assertEquals((byte)30, bytes[2]);
        assertEquals(3, bytes.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_BYTE,vector.subVector(0, 0).get(0));
        assertEquals(NULL_BYTE,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        byte[] bytes3 = vector.subVector(0, 1).toArray();
        assertEquals(1,bytes3.length);
        assertEquals((byte)10,bytes3[0]);

        assertEquals(NULL_BYTE,vector.subVector(0, 1).get(1));
        assertEquals(NULL_BYTE,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        byte[] bytes1 = vector.subVector(1, 2).toArray();
        assertEquals(1,bytes1.length);
        assertEquals((byte)20,bytes1[0]);
        assertEquals(NULL_BYTE,vector.subVector(0, 1).get(1));
        assertEquals(NULL_BYTE,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        byte[] bytes2 = vector.subVector(1, 3).toArray();
        assertEquals(2,bytes2.length);
        assertEquals((byte)20,bytes2[0]);
        assertEquals((byte)30,bytes2[1]);
        assertEquals(NULL_BYTE,vector.subVector(1, 3).get(2));
        assertEquals(NULL_BYTE,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        ByteVector vector = new ByteVectorDirect((byte) 10, (byte) 20, (byte) 30);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                byte result[]=new byte[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_BYTE : vector.get(i);
                }

                checkSubArray(vector, start, end, result);
            }
        }

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                for (int start2=-4; start2<=4; start2++){
                    for (int end2=-1; end2<=7; end2++){
                        if (start>end || start2>end2){
                            continue;
                        }

                        byte result[]=new byte[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_BYTE : vector.get(i);
                        }

                        byte result2[]=new byte[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_BYTE : result[i];
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

    private void checkSubArray(ByteVector vector, int start, int end, byte result[]){
        ByteVector subArray = vector.subVector(start, end);
        byte array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(ByteVector vector, int start, int end, int start2, int end2, byte result[]){
        ByteVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        byte array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
