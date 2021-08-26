/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.db.v2.dbarrays.DbArrayColumnWrapper;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.IntegerArraySource;
import io.deephaven.db.v2.utils.Index;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class DbArrayTest extends TestCase {

    public void testDbArrayColumnWrapper() {
        // noinspection unchecked
        DbArray dbArray = new DbArrayColumnWrapper(
            ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new String[] {"a", "b", "c"}),
            Index.FACTORY.getIndexByRange(0, 2));
        assertEquals(3, dbArray.size());
        assertEquals("a", dbArray.get(0));
        assertEquals("b", dbArray.get(1));
        assertEquals("c", dbArray.get(2));
        assertEquals(null, dbArray.get(3));
        assertEquals(null, dbArray.get(-1));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(dbArray.toArray()));
        assertEquals(0, dbArray.subArray(0, 0).size());
        assertEquals(Arrays.asList(), Arrays.asList(dbArray.subArray(0, 0).toArray()));
        assertEquals(null, dbArray.subArray(0, 0).get(0));
        assertEquals(null, dbArray.subArray(0, 0).get(-1));

        assertEquals(1, dbArray.subArray(0, 1).size());
        assertEquals(Arrays.asList("a"), Arrays.asList(dbArray.subArray(0, 1).toArray()));
        assertEquals(null, dbArray.subArray(0, 1).get(1));
        assertEquals(null, dbArray.subArray(0, 1).get(-1));

        assertEquals(1, dbArray.subArray(1, 2).size());
        assertEquals(Arrays.asList("b"), Arrays.asList(dbArray.subArray(1, 2).toArray()));
        assertEquals(null, dbArray.subArray(0, 1).get(1));
        assertEquals(null, dbArray.subArray(0, 1).get(-1));

        assertEquals(2, dbArray.subArray(1, 3).size());
        assertEquals(Arrays.asList("b", "c"), Arrays.asList(dbArray.subArray(1, 3).toArray()));
        assertEquals(null, dbArray.subArray(1, 3).get(2));
        assertEquals(null, dbArray.subArray(0, 1).get(-1));

    }

    public void testDbArrayDirect() {
        DbArrayDirect dbArrayDirect = new DbArrayDirect<>("a", "b", "c");
        assertEquals(3, dbArrayDirect.size());
        assertEquals("a", dbArrayDirect.get(0));
        assertEquals("b", dbArrayDirect.get(1));
        assertEquals("c", dbArrayDirect.get(2));
        assertEquals(null, dbArrayDirect.get(3));
        assertEquals(null, dbArrayDirect.get(-1));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(dbArrayDirect.toArray()));
        assertEquals(0, dbArrayDirect.subArray(0, 0).size());
        assertEquals(Arrays.asList(), Arrays.asList(dbArrayDirect.subArray(0, 0).toArray()));
        assertEquals(null, dbArrayDirect.subArray(0, 0).get(0));
        assertEquals(null, dbArrayDirect.subArray(0, 0).get(-1));

        assertEquals(1, dbArrayDirect.subArray(0, 1).size());
        assertEquals(Arrays.asList("a"), Arrays.asList(dbArrayDirect.subArray(0, 1).toArray()));
        assertEquals(null, dbArrayDirect.subArray(0, 1).get(1));
        assertEquals(null, dbArrayDirect.subArray(0, 1).get(-1));

        assertEquals(1, dbArrayDirect.subArray(1, 2).size());
        assertEquals(Arrays.asList("b"), Arrays.asList(dbArrayDirect.subArray(1, 2).toArray()));
        assertEquals(null, dbArrayDirect.subArray(0, 1).get(1));
        assertEquals(null, dbArrayDirect.subArray(0, 1).get(-1));

        assertEquals(2, dbArrayDirect.subArray(1, 3).size());
        assertEquals(Arrays.asList("b", "c"),
            Arrays.asList(dbArrayDirect.subArray(1, 3).toArray()));
        assertEquals(null, dbArrayDirect.subArray(1, 3).get(2));
        assertEquals(null, dbArrayDirect.subArray(0, 1).get(-1));
    }

    public void testSubArray() {
        // noinspection unchecked
        DbArray dbArray = new DbArrayColumnWrapper(
            ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new Object[] {10, 20, 30}),
            Index.FACTORY.getIndexByRange(0, 2));

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                Object result[] = new Object[end - start];

                for (int i = start; i < end; i++) {
                    result[i - start] = (i < 0 || i >= dbArray.size()) ? null : dbArray.get(i);
                }

                checkSubArray(dbArray, start, end, result);
            }
        }

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                for (int start2 = -4; start2 <= 4; start2++) {
                    for (int end2 = -1; end2 <= 7; end2++) {
                        if (start > end || start2 > end2) {
                            continue;
                        }

                        Object result[] = new Object[end - start];

                        for (int i = start; i < end; i++) {
                            result[i - start] =
                                (i < 0 || i >= dbArray.size()) ? null : dbArray.get(i);
                        }

                        Object result2[] = new Object[end2 - start2];

                        for (int i = start2; i < end2; i++) {
                            result2[i - start2] = (i < 0 || i >= result.length) ? null : result[i];
                        }

                        checkDoubleSubArray(dbArray, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    private void checkSubArray(DbArray dbArray, int start, int end, Object result[]) {
        DbArray subArray = dbArray.subArray(start, end);
        Object array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i = 0; i < result.length; i++) {
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    private void checkDoubleSubArray(DbArray dbArray, int start, int end, int start2, int end2,
        Object result[]) {
        DbArray subArray = dbArray.subArray(start, end);
        subArray = subArray.subArray(start2, end2);
        Object array[] = subArray.toArray();
        assertEquals(result.length, subArray.size());
        assertEquals(result.length, array.length);

        for (int i = 0; i < result.length; i++) {
            assertEquals(result[i], subArray.get(i));
            assertEquals(result[i], array[i]);
        }
    }

    public void testSubArrayByPositions() {
        final IntegerArraySource integerArraySource = new IntegerArraySource();
        integerArraySource.ensureCapacity(6);
        for (int ii = 0; ii < 6; ++ii) {
            integerArraySource.set(ii, (ii + 1) * 10);
        }
        DbArray<Integer> dbColumnArray =
            new DbArrayColumnWrapper<>(integerArraySource, Index.FACTORY.getIndexByRange(0, 5));
        DbIntArray dbDirectArray = new DbIntArrayDirect(10, 20, 30, 40, 50, 60);

        Random random = new Random(42);

        for (int step = 0; step < 50; ++step) {
            ArrayList<Integer> expected = new ArrayList<>();
            TLongList positions = new TLongArrayList();
            for (int ii = 0; ii < 6; ++ii) {
                if (random.nextBoolean()) {
                    expected.add(((ii + 1) * 10));
                    positions.add(ii);
                }
            }

            DbArray<Integer> columnResult =
                dbColumnArray.subArrayByPositions(positions.toArray(new long[positions.size()]));
            DbIntArray directResult =
                dbDirectArray.subArrayByPositions(positions.toArray(new long[positions.size()]));

            assertEquals(expected.size(), columnResult.size());
            assertEquals(expected.size(), directResult.size());

            for (int ii = 0; ii < expected.size(); ++ii) {
                assertEquals(expected.get(ii), columnResult.get(ii));
                assertEquals((int) expected.get(ii), directResult.get(ii));
            }
        }
    }

    /**
     * Verify that a DbArrayColumnWrapper can correctly invoke the 'getDirect' operation even when
     * one of the column sources is null.
     */
    public void testGetDirect() {
        DbArrayDirect dbArrayDirect = new DbArrayDirect<>("a", "b", "c");
        // noinspection unchecked
        DbArrayColumnWrapper dbArray = new DbArrayColumnWrapper(
            ArrayBackedColumnSource
                .getMemoryColumnSourceUntyped(new DbArrayBase[] {dbArrayDirect, null}),
            Index.FACTORY.getIndexByRange(0, 1));
        DbArrayBase base = dbArray.getDirect();
        assertEquals(2, base.intSize());
        assertTrue(DbArrayDirect.class.isAssignableFrom(base.getClass()));
        assertNull(((DbArrayDirect) base).get(1));
    }

}
