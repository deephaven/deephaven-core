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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatVectorTest extends TestCase {

    public void testVectorDirect() {
        FloatVector vector = new FloatVectorDirect((float)10, (float)20, (float)30);
        assertEquals(3, vector.size());
        assertEquals((float)10, vector.get(0));
        assertEquals((float)20, vector.get(1));
        assertEquals((float)30, vector.get(2));
        assertEquals(NULL_FLOAT,vector.get(3));
        assertEquals(NULL_FLOAT,vector.get(-1));
        float[] floats = vector.toArray();
        assertEquals((float)10, floats[0]);
        assertEquals((float)20, floats[1]);
        assertEquals((float)30, floats[2]);
        assertEquals(3, floats.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_FLOAT,vector.subVector(0, 0).get(0));
        assertEquals(NULL_FLOAT,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        float[] floats3 = vector.subVector(0, 1).toArray();
        assertEquals(1,floats3.length);
        assertEquals((float)10,floats3[0]);

        assertEquals(NULL_FLOAT,vector.subVector(0, 1).get(1));
        assertEquals(NULL_FLOAT,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        float[] floats1 = vector.subVector(1, 2).toArray();
        assertEquals(1,floats1.length);
        assertEquals((float)20,floats1[0]);
        assertEquals(NULL_FLOAT,vector.subVector(0, 1).get(1));
        assertEquals(NULL_FLOAT,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        float[] floats2 = vector.subVector(1, 3).toArray();
        assertEquals(2,floats2.length);
        assertEquals((float)20,floats2[0]);
        assertEquals((float)30,floats2[1]);
        assertEquals(NULL_FLOAT,vector.subVector(1, 3).get(2));
        assertEquals(NULL_FLOAT,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        FloatVector vector = new FloatVectorDirect((float) 10, (float) 20, (float) 30);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                float result[]=new float[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_FLOAT : vector.get(i);
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

                        float result[]=new float[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_FLOAT : vector.get(i);
                        }

                        float result2[]=new float[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_FLOAT : result[i];
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

    private void checkSubArray(FloatVector vector, int start, int end, float result[]){
        FloatVector subArray = vector.subVector(start, end);
        float array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(FloatVector vector, int start, int end, int start2, int end2, float result[]){
        FloatVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        float array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
