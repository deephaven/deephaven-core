/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorTest and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortVectorTest extends TestCase {

    public void testVectorDirect() {
        ShortVector vector = new ShortVectorDirect((short)10, (short)20, (short)30);
        assertEquals(3, vector.size());
        assertEquals((short)10, vector.get(0));
        assertEquals((short)20, vector.get(1));
        assertEquals((short)30, vector.get(2));
        assertEquals(NULL_SHORT,vector.get(3));
        assertEquals(NULL_SHORT,vector.get(-1));
        short[] shorts = vector.toArray();
        assertEquals((short)10, shorts[0]);
        assertEquals((short)20, shorts[1]);
        assertEquals((short)30, shorts[2]);
        assertEquals(3, shorts.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_SHORT,vector.subVector(0, 0).get(0));
        assertEquals(NULL_SHORT,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        short[] shorts3 = vector.subVector(0, 1).toArray();
        assertEquals(1,shorts3.length);
        assertEquals((short)10,shorts3[0]);

        assertEquals(NULL_SHORT,vector.subVector(0, 1).get(1));
        assertEquals(NULL_SHORT,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        short[] shorts1 = vector.subVector(1, 2).toArray();
        assertEquals(1,shorts1.length);
        assertEquals((short)20,shorts1[0]);
        assertEquals(NULL_SHORT,vector.subVector(0, 1).get(1));
        assertEquals(NULL_SHORT,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        short[] shorts2 = vector.subVector(1, 3).toArray();
        assertEquals(2,shorts2.length);
        assertEquals((short)20,shorts2[0]);
        assertEquals((short)30,shorts2[1]);
        assertEquals(NULL_SHORT,vector.subVector(1, 3).get(2));
        assertEquals(NULL_SHORT,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        ShortVector vector = new ShortVectorDirect((short) 10, (short) 20, (short) 30);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                short result[]=new short[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_SHORT : vector.get(i);
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

                        short result[]=new short[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_SHORT : vector.get(i);
                        }

                        short result2[]=new short[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_SHORT : result[i];
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

    private void checkSubArray(ShortVector vector, int start, int end, short result[]){
        ShortVector subArray = vector.subVector(start, end);
        short array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(ShortVector vector, int start, int end, int start2, int end2, short result[]){
        ShortVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        short array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
