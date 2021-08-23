/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayTest and regenerate
 ****************************************************************************************************************************/
package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbLongArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class DbLongArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        //noinspection unchecked
        DbLongArray dbArray = new DbLongArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new long[]{(long)10, (long)20, (long)30}),
                Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals((long)10, dbArray.get(0));
        assertEquals((long)20, dbArray.get(1));
        assertEquals((long)30, dbArray.get(2));
        assertEquals(NULL_LONG, dbArray.get(3));
        assertEquals(NULL_LONG, dbArray.get(-1));
        long[] longs = dbArray.toArray();
        assertEquals((long)10, longs[0]);
        assertEquals((long)20, longs[1]);
        assertEquals((long)30, longs[2]);
        assertEquals(3, longs.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_LONG, dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_LONG, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        long[] longs3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,longs3.length);
        assertEquals((long)10,longs3[0]);

        assertEquals(NULL_LONG, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_LONG, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        long[] longs1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,longs1.length);
        assertEquals((long)20,longs1[0]);
        assertEquals(NULL_LONG, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_LONG, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        long[] longs2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,longs2.length);
        assertEquals((long)20,longs2[0]);
        assertEquals((long)30,longs2[1]);
        assertEquals(NULL_LONG,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_LONG,dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbLongArray dbArray = new DbLongArrayDirect((long)10, (long)20, (long)30);
        assertEquals(3, dbArray.size());
        assertEquals((long)10, dbArray.get(0));
        assertEquals((long)20, dbArray.get(1));
        assertEquals((long)30, dbArray.get(2));
        assertEquals(NULL_LONG,dbArray.get(3));
        assertEquals(NULL_LONG,dbArray.get(-1));
        long[] longs = dbArray.toArray();
        assertEquals((long)10, longs[0]);
        assertEquals((long)20, longs[1]);
        assertEquals((long)30, longs[2]);
        assertEquals(3, longs.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_LONG,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_LONG,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        long[] longs3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,longs3.length);
        assertEquals((long)10,longs3[0]);

        assertEquals(NULL_LONG,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_LONG,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        long[] longs1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,longs1.length);
        assertEquals((long)20,longs1[0]);
        assertEquals(NULL_LONG,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_LONG,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        long[] longs2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,longs2.length);
        assertEquals((long)20,longs2[0]);
        assertEquals((long)30,longs2[1]);
        assertEquals(NULL_LONG,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_LONG,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        //noinspection unchecked
        DbLongArray dbArray = new DbLongArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new long[]{(long)10, (long)20, (long)30}),
                Index.FACTORY.getIndexByRange(0, 2));

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                long result[]=new long[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_LONG : dbArray.get(i);
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

                        long result[]=new long[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_LONG : dbArray.get(i);
                        }

                        long result2[]=new long[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_LONG : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbLongArray.type().clazz(), DbLongArray.class);
    }

    private void checkSubArray(DbLongArray dbArray, int start, int end, long result[]){
        DbLongArray subArray = dbArray.subArray(start, end);
        long array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbLongArray dbArray, int start, int end, int start2, int end2, long result[]){
        DbLongArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        long array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
