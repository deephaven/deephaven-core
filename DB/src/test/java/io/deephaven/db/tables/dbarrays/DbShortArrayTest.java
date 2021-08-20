/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayTest and regenerate
 ****************************************************************************************************************************/
package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbShortArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class DbShortArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        //noinspection unchecked
        DbShortArray dbArray = new DbShortArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new short[]{(short)10, (short)20, (short)30}),
                Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals((short)10, dbArray.get(0));
        assertEquals((short)20, dbArray.get(1));
        assertEquals((short)30, dbArray.get(2));
        assertEquals(NULL_SHORT, dbArray.get(3));
        assertEquals(NULL_SHORT, dbArray.get(-1));
        short[] shorts = dbArray.toArray();
        assertEquals((short)10, shorts[0]);
        assertEquals((short)20, shorts[1]);
        assertEquals((short)30, shorts[2]);
        assertEquals(3, shorts.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_SHORT, dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_SHORT, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        short[] shorts3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,shorts3.length);
        assertEquals((short)10,shorts3[0]);

        assertEquals(NULL_SHORT, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_SHORT, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        short[] shorts1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,shorts1.length);
        assertEquals((short)20,shorts1[0]);
        assertEquals(NULL_SHORT, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_SHORT, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        short[] shorts2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,shorts2.length);
        assertEquals((short)20,shorts2[0]);
        assertEquals((short)30,shorts2[1]);
        assertEquals(NULL_SHORT,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_SHORT,dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbShortArray dbArray = new DbShortArrayDirect((short)10, (short)20, (short)30);
        assertEquals(3, dbArray.size());
        assertEquals((short)10, dbArray.get(0));
        assertEquals((short)20, dbArray.get(1));
        assertEquals((short)30, dbArray.get(2));
        assertEquals(NULL_SHORT,dbArray.get(3));
        assertEquals(NULL_SHORT,dbArray.get(-1));
        short[] shorts = dbArray.toArray();
        assertEquals((short)10, shorts[0]);
        assertEquals((short)20, shorts[1]);
        assertEquals((short)30, shorts[2]);
        assertEquals(3, shorts.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_SHORT,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_SHORT,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        short[] shorts3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,shorts3.length);
        assertEquals((short)10,shorts3[0]);

        assertEquals(NULL_SHORT,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_SHORT,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        short[] shorts1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,shorts1.length);
        assertEquals((short)20,shorts1[0]);
        assertEquals(NULL_SHORT,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_SHORT,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        short[] shorts2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,shorts2.length);
        assertEquals((short)20,shorts2[0]);
        assertEquals((short)30,shorts2[1]);
        assertEquals(NULL_SHORT,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_SHORT,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        //noinspection unchecked
        DbShortArray dbArray = new DbShortArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new short[]{(short)10, (short)20, (short)30}),
                Index.FACTORY.getIndexByRange(0, 2));

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                short result[]=new short[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_SHORT : dbArray.get(i);
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

                        short result[]=new short[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_SHORT : dbArray.get(i);
                        }

                        short result2[]=new short[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_SHORT : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbShortArray.type().clazz(), DbShortArray.class);
    }

    private void checkSubArray(DbShortArray dbArray, int start, int end, short result[]){
        DbShortArray subArray = dbArray.subArray(start, end);
        short array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbShortArray dbArray, int start, int end, int start2, int end2, short result[]){
        DbShortArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        short array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
