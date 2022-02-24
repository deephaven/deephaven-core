/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongVectorTest extends TestCase {

    public void testVectorDirect() {
        LongVector vector = new LongVectorDirect((long)10, (long)20, (long)30);
        assertEquals(3, vector.size());
        assertEquals((long)10, vector.get(0));
        assertEquals((long)20, vector.get(1));
        assertEquals((long)30, vector.get(2));
        assertEquals(NULL_LONG,vector.get(3));
        assertEquals(NULL_LONG,vector.get(-1));
        long[] longs = vector.toArray();
        assertEquals((long)10, longs[0]);
        assertEquals((long)20, longs[1]);
        assertEquals((long)30, longs[2]);
        assertEquals(3, longs.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_LONG,vector.subVector(0, 0).get(0));
        assertEquals(NULL_LONG,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        long[] longs3 = vector.subVector(0, 1).toArray();
        assertEquals(1,longs3.length);
        assertEquals((long)10,longs3[0]);

        assertEquals(NULL_LONG,vector.subVector(0, 1).get(1));
        assertEquals(NULL_LONG,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        long[] longs1 = vector.subVector(1, 2).toArray();
        assertEquals(1,longs1.length);
        assertEquals((long)20,longs1[0]);
        assertEquals(NULL_LONG,vector.subVector(0, 1).get(1));
        assertEquals(NULL_LONG,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        long[] longs2 = vector.subVector(1, 3).toArray();
        assertEquals(2,longs2.length);
        assertEquals((long)20,longs2[0]);
        assertEquals((long)30,longs2[1]);
        assertEquals(NULL_LONG,vector.subVector(1, 3).get(2));
        assertEquals(NULL_LONG,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        LongVector vector = new LongVectorDirect((long) 10, (long) 20, (long) 30);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                long result[]=new long[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_LONG : vector.get(i);
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

                        long result[]=new long[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_LONG : vector.get(i);
                        }

                        long result2[]=new long[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_LONG : result[i];
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

    private void checkSubArray(LongVector vector, int start, int end, long result[]){
        LongVector subArray = vector.subVector(start, end);
        long array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(LongVector vector, int start, int end, int start2, int end2, long result[]){
        LongVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        long array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
