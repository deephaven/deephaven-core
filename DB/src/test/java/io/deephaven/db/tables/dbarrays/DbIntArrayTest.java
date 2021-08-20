/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayTest and regenerate
 ****************************************************************************************************************************/
package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbIntArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class DbIntArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        //noinspection unchecked
        DbIntArray dbArray = new DbIntArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new int[]{(int)10, (int)20, (int)30}),
                Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals((int)10, dbArray.get(0));
        assertEquals((int)20, dbArray.get(1));
        assertEquals((int)30, dbArray.get(2));
        assertEquals(NULL_INT, dbArray.get(3));
        assertEquals(NULL_INT, dbArray.get(-1));
        int[] ints = dbArray.toArray();
        assertEquals((int)10, ints[0]);
        assertEquals((int)20, ints[1]);
        assertEquals((int)30, ints[2]);
        assertEquals(3, ints.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_INT, dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_INT, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        int[] ints3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,ints3.length);
        assertEquals((int)10,ints3[0]);

        assertEquals(NULL_INT, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_INT, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        int[] ints1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,ints1.length);
        assertEquals((int)20,ints1[0]);
        assertEquals(NULL_INT, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_INT, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        int[] ints2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,ints2.length);
        assertEquals((int)20,ints2[0]);
        assertEquals((int)30,ints2[1]);
        assertEquals(NULL_INT,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_INT,dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbIntArray dbArray = new DbIntArrayDirect((int)10, (int)20, (int)30);
        assertEquals(3, dbArray.size());
        assertEquals((int)10, dbArray.get(0));
        assertEquals((int)20, dbArray.get(1));
        assertEquals((int)30, dbArray.get(2));
        assertEquals(NULL_INT,dbArray.get(3));
        assertEquals(NULL_INT,dbArray.get(-1));
        int[] ints = dbArray.toArray();
        assertEquals((int)10, ints[0]);
        assertEquals((int)20, ints[1]);
        assertEquals((int)30, ints[2]);
        assertEquals(3, ints.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_INT,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_INT,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        int[] ints3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,ints3.length);
        assertEquals((int)10,ints3[0]);

        assertEquals(NULL_INT,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_INT,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        int[] ints1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,ints1.length);
        assertEquals((int)20,ints1[0]);
        assertEquals(NULL_INT,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_INT,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        int[] ints2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,ints2.length);
        assertEquals((int)20,ints2[0]);
        assertEquals((int)30,ints2[1]);
        assertEquals(NULL_INT,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_INT,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        //noinspection unchecked
        DbIntArray dbArray = new DbIntArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new int[]{(int)10, (int)20, (int)30}),
                Index.FACTORY.getIndexByRange(0, 2));

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                int result[]=new int[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_INT : dbArray.get(i);
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

                        int result[]=new int[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_INT : dbArray.get(i);
                        }

                        int result2[]=new int[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_INT : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbIntArray.type().clazz(), DbIntArray.class);
    }

    private void checkSubArray(DbIntArray dbArray, int start, int end, int result[]){
        DbIntArray subArray = dbArray.subArray(start, end);
        int array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbIntArray dbArray, int start, int end, int start2, int end2, int result[]){
        DbIntArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        int array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
