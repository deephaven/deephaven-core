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

import static io.deephaven.util.QueryConstants.NULL_INT;

public class IntVectorTest extends TestCase {

    public void testVectorDirect() {
        IntVector vector = new IntVectorDirect((int)10, (int)20, (int)30);
        assertEquals(3, vector.size());
        assertEquals((int)10, vector.get(0));
        assertEquals((int)20, vector.get(1));
        assertEquals((int)30, vector.get(2));
        assertEquals(NULL_INT,vector.get(3));
        assertEquals(NULL_INT,vector.get(-1));
        int[] ints = vector.toArray();
        assertEquals((int)10, ints[0]);
        assertEquals((int)20, ints[1]);
        assertEquals((int)30, ints[2]);
        assertEquals(3, ints.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_INT,vector.subVector(0, 0).get(0));
        assertEquals(NULL_INT,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        int[] ints3 = vector.subVector(0, 1).toArray();
        assertEquals(1,ints3.length);
        assertEquals((int)10,ints3[0]);

        assertEquals(NULL_INT,vector.subVector(0, 1).get(1));
        assertEquals(NULL_INT,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        int[] ints1 = vector.subVector(1, 2).toArray();
        assertEquals(1,ints1.length);
        assertEquals((int)20,ints1[0]);
        assertEquals(NULL_INT,vector.subVector(0, 1).get(1));
        assertEquals(NULL_INT,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        int[] ints2 = vector.subVector(1, 3).toArray();
        assertEquals(2,ints2.length);
        assertEquals((int)20,ints2[0]);
        assertEquals((int)30,ints2[1]);
        assertEquals(NULL_INT,vector.subVector(1, 3).get(2));
        assertEquals(NULL_INT,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        IntVector vector = new IntVectorDirect((int) 10, (int) 20, (int) 30);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                int result[]=new int[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_INT : vector.get(i);
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

                        int result[]=new int[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_INT : vector.get(i);
                        }

                        int result2[]=new int[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_INT : result[i];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(IntVector.type().clazz(), IntVector.class);
    }

    private void checkSubArray(IntVector vector, int start, int end, int result[]){
        IntVector subArray = vector.subVector(start, end);
        int array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(IntVector vector, int start, int end, int start2, int end2, int result[]){
        IntVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        int array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
