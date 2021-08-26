/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

public class DbBooleanArrayTest extends TestCase {

    public void testDbArrayDirect() {
        DbBooleanArray dbArray = new DbBooleanArrayDirect(true, false, true);
        assertEquals(3, dbArray.size());
        assertEquals(Boolean.TRUE, dbArray.get(0));
        assertEquals(Boolean.FALSE, dbArray.get(1));
        assertEquals(Boolean.TRUE, dbArray.get(2));
        assertEquals(NULL_BOOLEAN,dbArray.get(3));
        assertEquals(NULL_BOOLEAN,dbArray.get(-1));
        Boolean[] booleans = dbArray.toArray();
        assertEquals(Boolean.TRUE, booleans[0]);
        assertEquals(Boolean.FALSE, booleans[1]);
        assertEquals(Boolean.TRUE, booleans[2]);
        assertEquals(3, booleans.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        Boolean[] booleans3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,booleans3.length);
        assertEquals(Boolean.TRUE,booleans3[0]);

        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        Boolean[] booleans1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,booleans1.length);
        assertEquals(Boolean.FALSE,booleans1[0]);
        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        Boolean[] booleans2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,booleans2.length);
        assertEquals(Boolean.FALSE,booleans2[0]);
        assertEquals(Boolean.TRUE,booleans2[1]);
        assertEquals(NULL_BOOLEAN,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_BOOLEAN,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        DbBooleanArray dbArray = new DbBooleanArrayDirect(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE);

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                Boolean result[]=new Boolean[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_BOOLEAN : dbArray.get(i);
                }

                checkSubArray(dbArray, start, end, result);
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
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_BOOLEAN : dbArray.get(i);
                        }

                        Boolean result2[]=new Boolean[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_BOOLEAN : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbBooleanArray.type().clazz(), DbBooleanArray.class);
    }

    private void checkSubArray(DbBooleanArray dbArray, int start, int end, Boolean result[]){
        DbBooleanArray subArray = dbArray.subArray(start, end);
        Boolean array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbBooleanArray dbArray, int start, int end, int start2, int end2, Boolean result[]){
        DbBooleanArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        Boolean array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
