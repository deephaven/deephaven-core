<#--
  Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
-->

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Sort.*;

/**
 * Test Sort.
 */
@SuppressWarnings({"ConstantConditions", "rawtypes", "RedundantCast", "RedundantArrayCreation", "unchecked"})
public class TestSort extends BaseArrayTestCase {

    //////////////////////////// Object ////////////////////////////

    public void testObjSort() {
        final ObjectVector<ComparableExtended> comparableExtendedObjectVector = new ObjectVectorDirect<>(null, new ComparableExtended(1d), new ComparableExtended(12d), new ComparableExtended(0d)
                , new ComparableExtended(9.4d), null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d));
        ComparableExtended[] sort = sortObj(comparableExtendedObjectVector);
        ComparableExtended[] expected = new ComparableExtended[]{null, null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d),
                new ComparableExtended(0d), new ComparableExtended(1d), new ComparableExtended(9.4d), new ComparableExtended(12d)};
        assertEquals(expected, sort);

        final BigDecimal[] bigDecimals = new BigDecimal[]{null, BigDecimal.valueOf(2.369), BigDecimal.valueOf(26), BigDecimal.valueOf(Float.MAX_VALUE), null, BigDecimal.valueOf(Long.MIN_VALUE)
                , BigDecimal.valueOf(-2395.365)};
        BigDecimal[] expectedBDs = new BigDecimal[]{null, null, BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(-2395.365), BigDecimal.valueOf(2.369), BigDecimal.valueOf(26), BigDecimal.valueOf(Float.MAX_VALUE)
        };
        BigDecimal[] sortedArray = sortObj(bigDecimals);
        assertEquals(expectedBDs, sortedArray);

        // check that functions can be resolved with varargs
        assertEquals(expectedBDs, sortObj(null, null, BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(-2395.365), BigDecimal.valueOf(2.369), BigDecimal.valueOf(26), BigDecimal.valueOf(Float.MAX_VALUE)));
    }

    public void testObjSortDescending() {
        final ObjectVector<ComparableExtended> comparableExtendedObjectVector = new ObjectVectorDirect<>(null, new ComparableExtended(1d), new ComparableExtended(12d), new ComparableExtended(0d)
                , new ComparableExtended(9.4d), null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d));

        ComparableExtended[] sort = sortDescendingObj(comparableExtendedObjectVector);
        ComparableExtended[] expected = new ComparableExtended[]{new ComparableExtended(12d), new ComparableExtended(9.4d), new ComparableExtended(1d),
                new ComparableExtended(0d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d), new ComparableExtended(-5.6d), null, null};
        assertEquals(expected, sort);

        final BigDecimal[] bigDecimals = new BigDecimal[]{null, BigDecimal.valueOf(2.369), BigDecimal.valueOf(26), BigDecimal.valueOf(Float.MAX_VALUE), null, BigDecimal.valueOf(Long.MIN_VALUE)
                , BigDecimal.valueOf(-2395.365)};
        BigDecimal[] expectedBDs = new BigDecimal[]{BigDecimal.valueOf(Float.MAX_VALUE), BigDecimal.valueOf(26), BigDecimal.valueOf(2.369), BigDecimal.valueOf(-2395.365), BigDecimal.valueOf(Long.MIN_VALUE), null, null};
        BigDecimal[] sortedArray = sortDescendingObj(bigDecimals);
        assertEquals(expectedBDs, sortedArray);

        // check that functions can be resolved with varargs
        assertEquals(expectedBDs, sortDescendingObj(BigDecimal.valueOf(Float.MAX_VALUE), BigDecimal.valueOf(26), BigDecimal.valueOf(2.369), BigDecimal.valueOf(-2395.365), BigDecimal.valueOf(Long.MIN_VALUE), null, null));
    }

    public void testObjSortExceptions() {
        //sort
        ObjectVector vectorToSort = null;

        Object[] sort = sortObj(vectorToSort);
        assertNull(sort);

        BigDecimal[] bd = null;
        BigDecimal[] sortedNumbers = sortObj(bd);
        assertNull(sortedNumbers);

        sort = sortObj(new ObjectVectorDirect<ComparableExtended>());
        assertEquals(new ComparableExtended[0], sort);

        bd = new BigDecimal[]{};
        sortedNumbers = sortObj(bd);
        assertTrue(ArrayUtils.isEmpty(sortedNumbers));

        //sortDescending

        sort = sortDescendingObj(vectorToSort);
        assertNull(sort);

        bd = null;
        sortedNumbers = sortDescendingObj(bd);
        assertNull(sortedNumbers);

        sort = sortDescendingObj(new ObjectVectorDirect<ComparableExtended>());
        assertEquals(new ComparableExtended[0], sort);

        bd = new BigDecimal[]{};
        sortedNumbers = sortDescendingObj(bd);
        assertTrue(ArrayUtils.isEmpty(sortedNumbers));
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    public void test${pt.boxed}Sort() {
        final ${pt.primitive}[] ${pt.primitive}s = new ${pt.primitive}[]{1, -5, -2, -2, 96, 0, 12, ${pt.null}, ${pt.null}};
        final ${pt.boxed}[] ${pt.boxed}s = new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})-5, (${pt.primitive})-2, (${pt.primitive})-2, (${pt.primitive})96, (${pt.primitive})0, (${pt.primitive})12, (${pt.primitive})${pt.null}, (${pt.primitive})${pt.null}};

        final ${pt.primitive}[] sort = sort(new ${pt.vectorDirect}(${pt.primitive}s));
        final ${pt.primitive}[] expected = new ${pt.primitive}[]{${pt.null}, ${pt.null}, -5, -2, -2, 0, 1, 12, 96};
        assertEquals(expected, sort);

        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -5, -2, -2, 0, 1, 12, 96}, sort(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[]{${pt.null}, ${pt.null}, -5, -2, -2, 0, 1, 12, 96}, sort(${pt.boxed}s));

        assertNull(sort((${pt.vector})null));
        assertNull(sort((${pt.primitive}[])null));
        assertNull(sort((${pt.boxed}[])null));
        assertEquals(new ${pt.primitive}[]{}, sort(new ${pt.vectorDirect}()));
        assertEquals(new ${pt.primitive}[]{}, sort(new ${pt.primitive}[]{}));
        assertEquals(new ${pt.primitive}[]{}, sort(new ${pt.boxed}[]{}));
    }

    public void test${pt.boxed}Rank() {
        final ${pt.primitive}[] ${pt.primitive}s = new ${pt.primitive}[]{1, -5, -2, -2, 96, 0, 12, ${pt.null}, ${pt.null}};
        final ${pt.boxed}[] ${pt.boxed}s = new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})-5, (${pt.primitive})-2, (${pt.primitive})-2, (${pt.primitive})96, (${pt.primitive})0, (${pt.primitive})12, (${pt.primitive})${pt.null}, (${pt.primitive})${pt.null}};

        final int[] sort = rank(new ${pt.vectorDirect}(${pt.primitive}s));
        final int[] expected = new int[]{7, 8, 1, 2, 3, 5, 0, 6, 4};
        assertEquals(expected, sort);

        assertEquals(expected, rank(${pt.primitive}s));
        assertEquals(expected, rank(${pt.boxed}s));

        assertNull(rank((${pt.vector})null));
        assertNull(rank((${pt.primitive}[])null));
        assertNull(rank((${pt.boxed}[])null));
        assertEquals(new int[]{}, rank(new ${pt.vectorDirect}()));
        assertEquals(new int[]{}, rank(new ${pt.primitive}[]{}));
        assertEquals(new int[]{}, rank(new ${pt.boxed}[]{}));
    }

    public void test${pt.boxed}SortDescending() {
        final ${pt.primitive}[] ${pt.primitive}s = new ${pt.primitive}[]{1, -5, -2, -2, 96, 0, 12, ${pt.null}, ${pt.null}};
        final ${pt.boxed}[] ${pt.boxed}s = new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})-5, (${pt.primitive})-2, (${pt.primitive})-2, (${pt.primitive})96, (${pt.primitive})0, (${pt.primitive})12, (${pt.primitive})${pt.null}, (${pt.primitive})${pt.null}};

        final ${pt.primitive}[] sort = sortDescending(new ${pt.vectorDirect}(${pt.primitive}s));
        final ${pt.primitive}[] expected = new ${pt.primitive}[]{96, 12, 1, 0, -2, -2, -5, ${pt.null}, ${pt.null}};
        assertEquals(expected, sort);

        assertEquals(new ${pt.primitive}[]{96, 12, 1, 0, -2, -2, -5, ${pt.null}, ${pt.null}}, sortDescending(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[]{96, 12, 1, 0, -2, -2, -5, ${pt.null}, ${pt.null}}, sortDescending(${pt.boxed}s));

        assertNull(sortDescending((${pt.vector})null));
        assertNull(sortDescending((${pt.primitive}[])null));
        assertNull(sortDescending((${pt.boxed}[])null));
        assertEquals(new ${pt.primitive}[]{}, sortDescending(new ${pt.vectorDirect}()));
        assertEquals(new ${pt.primitive}[]{}, sortDescending(new ${pt.primitive}[]{}));
        assertEquals(new ${pt.primitive}[]{}, sortDescending(new ${pt.boxed}[]{}));
    }

    public void test${pt.boxed}rankDescending() {
        final ${pt.primitive}[] ${pt.primitive}s = new ${pt.primitive}[]{1, -5, -2, -2, 96, 0, 12, ${pt.null}, ${pt.null}};
        final ${pt.boxed}[] ${pt.boxed}s = new ${pt.boxed}[]{(${pt.primitive})1, (${pt.primitive})-5, (${pt.primitive})-2, (${pt.primitive})-2, (${pt.primitive})96, (${pt.primitive})0, (${pt.primitive})12, (${pt.primitive})${pt.null}, (${pt.primitive})${pt.null}};

        final int[] sort = rankDescending(new ${pt.vectorDirect}(${pt.primitive}s));
        final int[] expected = new int[]{4, 6, 0, 5, 2, 3, 1, 7, 8};
        assertEquals(expected, sort);

        assertEquals(expected, rankDescending(${pt.primitive}s));
        assertEquals(expected, rankDescending(${pt.boxed}s));

        assertNull(rankDescending((${pt.vector})null));
        assertNull(rankDescending((${pt.primitive}[])null));
        assertNull(rankDescending((${pt.boxed}[])null));
        assertEquals(new int[]{}, rankDescending(new ${pt.vectorDirect}()));
        assertEquals(new int[]{}, rankDescending(new ${pt.primitive}[]{}));
        assertEquals(new int[]{}, rankDescending(new ${pt.boxed}[]{}));
    }

    public void test${pt.boxed}SortExceptions() {
        ${pt.vector} db${pt.boxed}Array = null;
        ${pt.primitive}[] sort = sort(db${pt.boxed}Array);
        assertNull(sort);

        ${pt.primitive}[] ${pt.primitive}s = null;
        ${pt.primitive}[] sortArray = sort(${pt.primitive}s);
        assertNull(sortArray);

        ${pt.primitive}s = new ${pt.primitive}[]{};
        sort = sort(new ${pt.vectorDirect}(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[0], sort);

        sortArray = sort(${pt.primitive}s);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void test${pt.boxed}RankExceptions() {
        ${pt.vector} db${pt.boxed}Array = null;
        int[] sort = rank(db${pt.boxed}Array);
        assertNull(sort);

        ${pt.primitive}[] ${pt.primitive}s = null;
        int[] sortArray = rank(${pt.primitive}s);
        assertNull(sortArray);

        ${pt.primitive}s = new ${pt.primitive}[]{};
        sort = rank(new ${pt.vectorDirect}(${pt.primitive}s));
        assertEquals(new int[0], sort);

        sortArray = rank(${pt.primitive}s);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void test${pt.boxed}SortDescendingExceptions() {
        ${pt.vector} db${pt.boxed}Array = null;
        ${pt.primitive}[] sort = sortDescending(db${pt.boxed}Array);
        assertNull(sort);

        ${pt.primitive}[] ${pt.primitive}s = null;
        ${pt.primitive}[] sortArray = sortDescending(${pt.primitive}s);
        assertNull(sortArray);

        ${pt.primitive}s = new ${pt.primitive}[]{};
        sort = sortDescending(new ${pt.vectorDirect}(${pt.primitive}s));
        assertEquals(new ${pt.primitive}[0], sort);

        sortArray = sortDescending(${pt.primitive}s);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    public void test${pt.boxed}rankDescendingExceptions() {
        ${pt.vector} db${pt.boxed}Array = null;
        int[] sort = rankDescending(db${pt.boxed}Array);
        assertNull(sort);

        ${pt.primitive}[] ${pt.primitive}s = null;
        int[] sortArray = rankDescending(${pt.primitive}s);
        assertNull(sortArray);

        ${pt.primitive}s = new ${pt.primitive}[]{};
        sort = rankDescending(new ${pt.vectorDirect}(${pt.primitive}s));
        assertEquals(new int[0], sort);

        sortArray = rankDescending(${pt.primitive}s);
        assertTrue(ArrayUtils.isEmpty(sortArray));
    }

    </#if>
    </#list>
}