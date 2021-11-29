/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

public class BooleanVectorTest extends TestCase {

    public void testDirect() {
        BooleanVector vector = new BooleanVectorDirect(true, false, true);
        assertEquals(3, vector.size());
        assertEquals(Boolean.TRUE, vector.get(0));
        assertEquals(Boolean.FALSE, vector.get(1));
        assertEquals(Boolean.TRUE, vector.get(2));
        assertEquals(NULL_BOOLEAN,vector.get(3));
        assertEquals(NULL_BOOLEAN,vector.get(-1));
        Boolean[] booleans = vector.toArray();
        assertEquals(Boolean.TRUE, booleans[0]);
        assertEquals(Boolean.FALSE, booleans[1]);
        assertEquals(Boolean.TRUE, booleans[2]);
        assertEquals(3, booleans.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_BOOLEAN,vector.subVector(0, 0).get(0));
        assertEquals(NULL_BOOLEAN,vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        Boolean[] booleans3 = vector.subVector(0, 1).toArray();
        assertEquals(1,booleans3.length);
        assertEquals(Boolean.TRUE,booleans3[0]);

        assertEquals(NULL_BOOLEAN,vector.subVector(0, 1).get(1));
        assertEquals(NULL_BOOLEAN,vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        Boolean[] booleans1 = vector.subVector(1, 2).toArray();
        assertEquals(1,booleans1.length);
        assertEquals(Boolean.FALSE,booleans1[0]);
        assertEquals(NULL_BOOLEAN,vector.subVector(0, 1).get(1));
        assertEquals(NULL_BOOLEAN,vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        Boolean[] booleans2 = vector.subVector(1, 3).toArray();
        assertEquals(2,booleans2.length);
        assertEquals(Boolean.FALSE,booleans2[0]);
        assertEquals(Boolean.TRUE,booleans2[1]);
        assertEquals(NULL_BOOLEAN,vector.subVector(1, 3).get(2));
        assertEquals(NULL_BOOLEAN,vector.subVector(0, 1).get(-1));
    }

    public void testSubArray() {
        BooleanVector vector = new BooleanVectorDirect(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                Boolean result[]=new Boolean[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=vector.size()) ? NULL_BOOLEAN : vector.get(i);
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

                        Boolean result[]=new Boolean[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=vector.size()) ? NULL_BOOLEAN : vector.get(i);
                        }

                        Boolean result2[]=new Boolean[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_BOOLEAN : result[i];
                        }

                        checkDoubleSubArray(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(BooleanVector.type().clazz(), BooleanVector.class);
    }

    private void checkSubArray(BooleanVector vector, int start, int end, Boolean result[]){
        BooleanVector subArray = vector.subVector(start, end);
        Boolean array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(BooleanVector vector, int start, int end, int start2, int end2, Boolean result[]){
        BooleanVector subArray = vector.subVector(start, end);
        subArray = subArray.subVector(start2, end2);
        Boolean array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
