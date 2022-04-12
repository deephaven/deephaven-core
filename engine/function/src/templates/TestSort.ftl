/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.querylibrary;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.querylibrary.Sort.*;

/**
 * Test Sort.
 */
@SuppressWarnings("ConstantConditions")
public class TestSort extends BaseArrayTestCase {

    //////////////////////////// Object ////////////////////////////

    public void testGenericSort() {
        final ObjectVector<ComparableExtended> comparableExtendedObjectVector = new ObjectVectorDirect<>(null, new ComparableExtended(1d), new ComparableExtended(12d), new ComparableExtended(0d)
                , new ComparableExtended(9.4d), null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d));
        ComparableExtended[] sort = sort(comparableExtendedObjectVector);
        ComparableExtended[] expected = new ComparableExtended[]{null, null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d),
                new ComparableExtended(0d), new ComparableExtended(1d), new ComparableExtended(9.4d), new ComparableExtended(12d)};
        assertEquals(expected, sort);

        final BigDecimal[] bigDecimals = new BigDecimal[]{null, new BigDecimal("2.369"), new BigDecimal(26), new BigDecimal(Float.MAX_VALUE), null, new BigDecimal(Long.MIN_VALUE)
                , new BigDecimal(-2395.365)};
        BigDecimal[] expectedBDs = new BigDecimal[]{null, null, new BigDecimal(Long.MIN_VALUE), new BigDecimal(-2395.365), new BigDecimal("2.369"), new BigDecimal(26), new BigDecimal(Float.MAX_VALUE)
        };
        BigDecimal[] sortedArray = sort(bigDecimals);
        assertEquals(expectedBDs, sortedArray);
    }

    public void testGenericSortDescending() {
        final ObjectVector<ComparableExtended> comparableExtendedObjectVector = new ObjectVectorDirect<>(null, new ComparableExtended(1d), new ComparableExtended(12d), new ComparableExtended(0d)
                , new ComparableExtended(9.4d), null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d));

        ComparableExtended[] sort = sortDescending(comparableExtendedObjectVector);
        ComparableExtended[] expected = new ComparableExtended[]{new ComparableExtended(12d), new ComparableExtended(9.4d), new ComparableExtended(1d),
                new ComparableExtended(0d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d), new ComparableExtended(-5.6d), null, null};
        assertEquals(expected, sort);

        final BigDecimal[] bigDecimals = new BigDecimal[]{null, new BigDecimal("2.369"), new BigDecimal(26), new BigDecimal(Float.MAX_VALUE), null, new BigDecimal(Long.MIN_VALUE)
                , new BigDecimal(-2395.365)};
        BigDecimal[] expectedBDs = new BigDecimal[]{new BigDecimal(Float.MAX_VALUE), new BigDecimal(26), new BigDecimal("2.369"), new BigDecimal(-2395.365), new BigDecimal(Long.MIN_VALUE), null, null};
        BigDecimal[] sortedArray = sortDescending(bigDecimals);
        assertEquals(expectedBDs, sortedArray);
    }

    public void testGenericSortExceptions() {
        //sort
        ObjectVector dbArrayToSort = null;

        Object[] sort = sort(dbArrayToSort);
        assertNull(sort);

        BigDecimal[] bd = null;
        BigDecimal[] sortedNumbers = sort(bd);
        assertNull(sortedNumbers);

        sort = sort(new ObjectVectorDirect<ComparableExtended>());
        assertEquals(new ComparableExtended[0], sort);

        bd = new BigDecimal[]{};
        sortedNumbers = sort(bd);
        assertTrue(ArrayUtils.isEmpty(sortedNumbers));

        //sortDescending

        sort = sortDescending(dbArrayToSort);
        assertNull(sort);

        bd = null;
        sortedNumbers = sortDescending(bd);
        assertNull(sortedNumbers);

        sort = sortDescending(new ObjectVectorDirect<ComparableExtended>());
        assertEquals(new ComparableExtended[0], sort);

        bd = new BigDecimal[]{};
        sortedNumbers = sortDescending(bd);
        assertTrue(ArrayUtils.isEmpty(sortedNumbers));
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    public void test${pt.boxed}Sort() {
        final ${pt.primitive}[] ${pt.primitive}s = new ${pt.primitive}[]{1, -5, -2, -2, 96, 0, 12, ${pt.null}, ${pt.null}};
        final ${pt.boxed}[] ${pt.boxed}s = new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})-5, (${pt.primitive})-2, (${pt.primitive})-2, (${pt.primitive})96, (${pt.primitive})0, (${pt.primitive})12, (${pt.primitive})${pt.null}, (${pt.primitive})${pt.null}};

        final ${pt.primitive}[] sort = sort(new ${pt.dbArrayDirect}(${pt.primitive}s));
        final ${pt.primitive}[] expected = new ${pt.primitive}[]{${pt.null}, ${pt.null}, -5, -2, -2, 0, 1, 12, 96};
        assertEquals(expected, sort);

        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -5, -2, -2, 0, 1, 12, 96}, sort(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -5, -2, -2, 0, 1, 12, 96}, sort(${pt.boxed}s));

        assertNull(sort((${pt.dbArray})null));
        assertNull(sort((${pt.primitive}[])null));
        assertNull(sort((${pt.boxed}[])null));
        assertEquals(new ${pt.primitive}[]{}, sort(new ${pt.dbArrayDirect}()));
        assertEquals(new ${pt.primitive}[]{}, sort(new ${pt.primitive}[]{}));
        assertEquals(new ${pt.primitive}[]{}, sort(new ${pt.boxed}[]{}));
    }

    public void test${pt.boxed}SortDescending() {
        final ${pt.primitive}[] ${pt.primitive}s = new ${pt.primitive}[]{1, -5, -2, -2, 96, 0, 12, ${pt.null}, ${pt.null}};
        final ${pt.boxed}[] ${pt.boxed}s = new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})-5, (${pt.primitive})-2, (${pt.primitive})-2, (${pt.primitive})96, (${pt.primitive})0, (${pt.primitive})12, (${pt.primitive})${pt.null}, (${pt.primitive})${pt.null}};

        final ${pt.primitive}[] sort = sortDescending(new ${pt.dbArrayDirect}(${pt.primitive}s));
        final ${pt.primitive}[] expected = new ${pt.primitive}[]{96, 12, 1, 0, -2, -2, -5, ${pt.null}, ${pt.null}};
        assertEquals(expected, sort);

        assertEquals(new ${pt.primitive}[]{96, 12, 1, 0, -2, -2, -5, ${pt.null}, ${pt.null}}, sortDescending(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[]{96, 12, 1, 0, -2, -2, -5, ${pt.null}, ${pt.null}}, sortDescending(${pt.boxed}s));

        assertNull(sortDescending((${pt.dbArray})null));
        assertNull(sortDescending((${pt.primitive}[])null));
        assertNull(sortDescending((${pt.boxed}[])null));
        assertEquals(new ${pt.primitive}[]{}, sortDescending(new ${pt.dbArrayDirect}()));
        assertEquals(new ${pt.primitive}[]{}, sortDescending(new ${pt.primitive}[]{}));
        assertEquals(new ${pt.primitive}[]{}, sortDescending(new ${pt.boxed}[]{}));
    }

    public void test${pt.boxed}SortsExceptions() {
        ${pt.dbArray} db${pt.boxed}Array = null;
        ${pt.primitive}[] sort = sort(db${pt.boxed}Array);
        assertNull(sort);

        ${pt.primitive}[] ${pt.primitive}s = null;
        ${pt.primitive}[] sortArray = sort(${pt.primitive}s);
        assertNull(sortArray);

        ${pt.primitive}s = new ${pt.primitive}[]{};
        sort = sort(new ${pt.dbArrayDirect}(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[0], sort);

        sortArray = sort(${pt.primitive}s);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void test${pt.boxed}SortDescendingExceptions() {
        ${pt.dbArray} db${pt.boxed}Array = null;
        ${pt.primitive}[] sort = sortDescending(db${pt.boxed}Array);
        assertNull(sort);

        ${pt.primitive}[] ${pt.primitive}s = null;
        ${pt.primitive}[] sortArray = sortDescending(${pt.primitive}s);
        assertNull(sortArray);

        ${pt.primitive}s = new ${pt.primitive}[]{};
        sort = sortDescending(new ${pt.dbArrayDirect}(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[0], sort);

        sortArray = sortDescending(${pt.primitive}s);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    
    </#if>
    </#list>
}