/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayTest and regenerate
 ****************************************************************************************************************************/
package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbFloatArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class DbFloatArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        //noinspection unchecked
        DbFloatArray dbArray = new DbFloatArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new float[]{(float)10, (float)20, (float)30}),
                Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals((float)10, dbArray.get(0));
        assertEquals((float)20, dbArray.get(1));
        assertEquals((float)30, dbArray.get(2));
        assertEquals(NULL_FLOAT, dbArray.get(3));
        assertEquals(NULL_FLOAT, dbArray.get(-1));
        float[] floats = dbArray.toArray();
        assertEquals((float)10, floats[0]);
        assertEquals((float)20, floats[1]);
        assertEquals((float)30, floats[2]);
        assertEquals(3, floats.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_FLOAT, dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_FLOAT, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        float[] floats3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,floats3.length);
        assertEquals((float)10,floats3[0]);

        assertEquals(NULL_FLOAT, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_FLOAT, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        float[] floats1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,floats1.length);
        assertEquals((float)20,floats1[0]);
        assertEquals(NULL_FLOAT, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_FLOAT, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        float[] floats2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,floats2.length);
        assertEquals((float)20,floats2[0]);
        assertEquals((float)30,floats2[1]);
        assertEquals(NULL_FLOAT,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbFloatArray dbArray = new DbFloatArrayDirect((float)10, (float)20, (float)30);
        assertEquals(3, dbArray.size());
        assertEquals((float)10, dbArray.get(0));
        assertEquals((float)20, dbArray.get(1));
        assertEquals((float)30, dbArray.get(2));
        assertEquals(NULL_FLOAT,dbArray.get(3));
        assertEquals(NULL_FLOAT,dbArray.get(-1));
        float[] floats = dbArray.toArray();
        assertEquals((float)10, floats[0]);
        assertEquals((float)20, floats[1]);
        assertEquals((float)30, floats[2]);
        assertEquals(3, floats.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        float[] floats3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,floats3.length);
        assertEquals((float)10,floats3[0]);

        assertEquals(NULL_FLOAT,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        float[] floats1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,floats1.length);
        assertEquals((float)20,floats1[0]);
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        float[] floats2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,floats2.length);
        assertEquals((float)20,floats2[0]);
        assertEquals((float)30,floats2[1]);
        assertEquals(NULL_FLOAT,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_FLOAT,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        //noinspection unchecked
        DbFloatArray dbArray = new DbFloatArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new float[]{(float)10, (float)20, (float)30}),
                Index.FACTORY.getIndexByRange(0, 2));

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                float result[]=new float[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_FLOAT : dbArray.get(i);
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

                        float result[]=new float[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_FLOAT : dbArray.get(i);
                        }

                        float result2[]=new float[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_FLOAT : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbFloatArray.type().clazz(), DbFloatArray.class);
    }

    private void checkSubArray(DbFloatArray dbArray, int start, int end, float result[]){
        DbFloatArray subArray = dbArray.subArray(start, end);
        float array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbFloatArray dbArray, int start, int end, int start2, int end2, float result[]){
        DbFloatArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        float array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
