/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbCharArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class DbCharArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        //noinspection unchecked
        DbCharArray dbArray = new DbCharArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new char[]{(char)10, (char)20, (char)30}),
                Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals((char)10, dbArray.get(0));
        assertEquals((char)20, dbArray.get(1));
        assertEquals((char)30, dbArray.get(2));
        assertEquals(NULL_CHAR, dbArray.get(3));
        assertEquals(NULL_CHAR, dbArray.get(-1));
        char[] chars = dbArray.toArray();
        assertEquals((char)10, chars[0]);
        assertEquals((char)20, chars[1]);
        assertEquals((char)30, chars[2]);
        assertEquals(3, chars.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_CHAR, dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_CHAR, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        char[] chars3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,chars3.length);
        assertEquals((char)10,chars3[0]);

        assertEquals(NULL_CHAR, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_CHAR, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        char[] chars1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,chars1.length);
        assertEquals((char)20,chars1[0]);
        assertEquals(NULL_CHAR, dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_CHAR, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        char[] chars2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,chars2.length);
        assertEquals((char)20,chars2[0]);
        assertEquals((char)30,chars2[1]);
        assertEquals(NULL_CHAR,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_CHAR,dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbCharArray dbArray = new DbCharArrayDirect((char)10, (char)20, (char)30);
        assertEquals(3, dbArray.size());
        assertEquals((char)10, dbArray.get(0));
        assertEquals((char)20, dbArray.get(1));
        assertEquals((char)30, dbArray.get(2));
        assertEquals(NULL_CHAR,dbArray.get(3));
        assertEquals(NULL_CHAR,dbArray.get(-1));
        char[] chars = dbArray.toArray();
        assertEquals((char)10, chars[0]);
        assertEquals((char)20, chars[1]);
        assertEquals((char)30, chars[2]);
        assertEquals(3, chars.length);
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(0, dbArray.subArray(0, 0).toArray().length);
        assertEquals(NULL_CHAR,dbArray.subArray(0, 0).get(0));
        assertEquals(NULL_CHAR,dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        char[] chars3 = dbArray.subArray(0, 1).toArray();
        assertEquals(1,chars3.length);
        assertEquals((char)10,chars3[0]);

        assertEquals(NULL_CHAR,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_CHAR,dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        char[] chars1 = dbArray.subArray(1, 2).toArray();
        assertEquals(1,chars1.length);
        assertEquals((char)20,chars1[0]);
        assertEquals(NULL_CHAR,dbArray.subArray(0, 1).get(1));
        assertEquals(NULL_CHAR,dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        char[] chars2 = dbArray.subArray(1, 3).toArray();
        assertEquals(2,chars2.length);
        assertEquals((char)20,chars2[0]);
        assertEquals((char)30,chars2[1]);
        assertEquals(NULL_CHAR,dbArray.subArray(1, 3).get(2));
        assertEquals(NULL_CHAR,dbArray.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        //noinspection unchecked
        DbCharArray dbArray = new DbCharArrayColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new char[]{(char)10, (char)20, (char)30}),
                Index.FACTORY.getIndexByRange(0, 2));

        for (int start=-4; start<=4; start++){
            for (int end=-1; end<=7; end++){
                if (start>end){
                    continue;
                }

                char result[]=new char[end-start];

                for (int i=start; i<end; i++){
                    result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_CHAR : dbArray.get(i);
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

                        char result[]=new char[end-start];

                        for (int i=start; i<end; i++){
                            result[i-start] =(i<0 || i>=dbArray.size()) ? NULL_CHAR : dbArray.get(i);
                        }

                        char result2[]=new char[end2-start2];

                        for (int i=start2; i<end2; i++){
                            result2[i-start2] =(i<0 || i>=result.length) ? NULL_CHAR : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    public void testType() {
        assertEquals(DbCharArray.type().clazz(), DbCharArray.class);
    }

    private void checkSubArray(DbCharArray dbArray, int start, int end, char result[]){
        DbCharArray subArray = dbArray.subArray(start, end);
        char array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbCharArray dbArray, int start, int end, int start2, int end2, char result[]){
        DbCharArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        char array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i=0; i<result.length; i++){
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }
}
