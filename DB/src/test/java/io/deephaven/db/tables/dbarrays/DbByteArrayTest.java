/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DbCharArrayTest and regenerate
 ****************************************************************************************************************************/
package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbByteArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class DbByteArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        //noinspection unchecked
        DbByteArray dbArray = new DbByteArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new byte[]{(byte)10, (byte)20, (byte)30}),
                Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals((byte)10, dbArray.get(0));
        assertEquals((byte)20, dbArray.get(1));
        assertEquals((byte)30, dbArray.get(2));
        assertEquals(NULL_BYTE, dbArray.get(3));
        assertEquals(NULL_BYTE, dbArray.get(-1));
        byte[] bytes = dbArray.toArray();
        assertEquals((byte)10, bytes[0]);
        assertEquals((byte)20, bytes[1]);
        assertEquals((byte)30, bytes[2]);
        assertEquals(3, bytes.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_BYTE, dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_BYTE, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        byte[] bytes3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,bytes3.length);
        assertEquals((byte)10,bytes3[0]);

        assertEquals(NULL_BYTE, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_BYTE, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        byte[] bytes1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,bytes1.length);
        assertEquals((byte)20,bytes1[0]);
        assertEquals(NULL_BYTE, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_BYTE, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        byte[] bytes2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,bytes2.length);
        assertEquals((byte)20,bytes2[0]);
        assertEquals((byte)30,bytes2[1]);
        assertEquals(NULL_BYTE,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_BYTE,dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbByteArray dbArray = new DbByteArrayDirect((byte)10, (byte)20, (byte)30);
        assertEquals(3, dbArray.size());
        assertEquals((byte)10, dbArray.get(0));
        assertEquals((byte)20, dbArray.get(1));
        assertEquals((byte)30, dbArray.get(2));
        assertEquals(NULL_BYTE,dbArray.get(3));
        assertEquals(NULL_BYTE,dbArray.get(-1));
        byte[] bytes = dbArray.toArray();
        assertEquals((byte)10, bytes[0]);
        assertEquals((byte)20, bytes[1]);
        assertEquals((byte)30, bytes[2]);
        assertEquals(3, bytes.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_BYTE,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_BYTE,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        byte[] bytes3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,bytes3.length);
        assertEquals((byte)10,bytes3[0]);

        assertEquals(NULL_BYTE,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_BYTE,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        byte[] bytes1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,bytes1.length);
        assertEquals((byte)20,bytes1[0]);
        assertEquals(NULL_BYTE,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_BYTE,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        byte[] bytes2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,bytes2.length);
        assertEquals((byte)20,bytes2[0]);
        assertEquals((byte)30,bytes2[1]);
        assertEquals(NULL_BYTE,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_BYTE,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        //noinspection unchecked
        DbByteArray dbArray = new DbByteArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new byte[]{(byte)10, (byte)20, (byte)30}),
                Index.FACTORY.getIndexByRange(0, 2));

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                byte result[]=new byte[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_BYTE : dbArray.get(i);
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

                        byte result[]=new byte[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_BYTE : dbArray.get(i);
                        }

                        byte result2[]=new byte[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_BYTE : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbByteArray.type().clazz(), DbByteArray.class);
    }

    private void checkSubArray(DbByteArray dbArray, int start, int end, byte result[]){
        DbByteArray subArray = dbArray.subArray(start, end);
        byte array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbByteArray dbArray, int start, int end, int start2, int end2, byte result[]){
        DbByteArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        byte array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
