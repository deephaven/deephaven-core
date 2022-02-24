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

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleVectorTest extends TestCase {

    public void testVectorDirect() {
        DoubleVector vector = new DoubleVectorDirect((double)10, (double)20, (double)30);
        assertEquals(3, vector.size());
        assertEquals((double)10, vector.get(0));
        assertEquals((double)20, vector.get(1));
        assertEquals((double)30, vector.get(2));
        assertEquals(NULL_DOUBLE,vector.get(3));
        assertEquals(NULL_DOUBLE,vector.get(-1));
        double[] doubles = vector.toArray();
        assertEquals((double)10, doubles[0]);
        assertEquals((double)20, doubles[1]);
        assertEquals((double)30, doubles[2]);
        assertEquals(3, doubles.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_DOUBLE,vector.subVector(0, 0).get(0));
        assertEquals(NULL_DOUBLE,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        double[] doubles3 = vector.subVector(0, 1).toArray();
        assertEquals(1,doubles3.length);
        assertEquals((double)10,doubles3[0]);

        assertEquals(NULL_DOUBLE,vector.subVector(0, 1).get(1));
        assertEquals(NULL_DOUBLE,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        double[] doubles1 = vector.subVector(1, 2).toArray();
        assertEquals(1,doubles1.length);
        assertEquals((double)20,doubles1[0]);
        assertEquals(NULL_DOUBLE,vector.subVector(0, 1).get(1));
        assertEquals(NULL_DOUBLE,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        double[] doubles2 = vector.subVector(1, 3).toArray();
        assertEquals(2,doubles2.length);
        assertEquals((double)20,doubles2[0]);
        assertEquals((double)30,doubles2[1]);
        assertEquals(NULL_DOUBLE,vector.subVector(1, 3).get(2));
        assertEquals(NULL_DOUBLE,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        DoubleVector vector = new DoubleVectorDirect((double) 10, (double) 20, (double) 30);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                double result[]=new double[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_DOUBLE : vector.get(i);
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

                        double result[]=new double[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_DOUBLE : vector.get(i);
                        }

                        double result2[]=new double[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_DOUBLE : result[i];
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

    private void checkSubArray(DoubleVector vector, int start, int end, double result[]){
        DoubleVector subArray = vector.subVector(start, end);
        double array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DoubleVector vector, int start, int end, int start2, int end2, double result[]){
        DoubleVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        double array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
