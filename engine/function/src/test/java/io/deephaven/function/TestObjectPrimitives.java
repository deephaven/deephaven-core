/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.deephaven.function.ObjectPrimitives.*;
import static io.deephaven.util.QueryConstants.*;

public class TestObjectPrimitives extends BaseArrayTestCase {

    public void testContains() {
        assertTrue(contains("ABCDE", "BCD"));
        assertFalse(contains("ABCDE", "XYZ"));
    }

    public void testIsNull() {
        assertFalse(isNull(new Object()));
        assertTrue(isNull(null));
    }

    public void testIsDBNull() {
        assertFalse(isDBNull(new Object()));
        assertTrue(isDBNull(null));

        assertFalse(isDBNull(true));
        assertTrue(isDBNull(NULL_BOOLEAN));

        assertFalse(isDBNull((char) 1));
        assertTrue(isDBNull(NULL_CHAR));

        assertFalse(isDBNull((byte) 1));
        assertTrue(isDBNull(NULL_BYTE));

        assertFalse(isDBNull((short) 1));
        assertTrue(isDBNull(NULL_SHORT));

        assertFalse(isDBNull((int) 1));
        assertTrue(isDBNull(NULL_INT));

        assertFalse(isDBNull((long) 1));
        assertTrue(isDBNull(NULL_LONG));

        assertFalse(isDBNull((float) 1));
        assertTrue(isDBNull(NULL_FLOAT));

        assertFalse(isDBNull((double) 1));
        assertTrue(isDBNull(NULL_DOUBLE));
    }

    public void testNullToValueScalar() {
        assertEquals(new Integer(7), nullToValue(new Integer(7), new Integer(3)));
        assertEquals(new Integer(3), nullToValue((Integer) null, new Integer(3)));
    }

    public void testNullToValueArray() {
        assertEquals(new Integer[]{new Integer(7), new Integer(3), new Integer(-5)},
                nullToValue(new ObjectVectorDirect<>(new Integer[]{new Integer(7), null, new Integer(-5)}), new Integer(3)));
    }

    public void testCount() {
        assertEquals(3, count(new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(0, count(new ObjectVectorDirect<Integer>()));
        assertEquals(0, count(new ObjectVectorDirect<Integer>(new Integer[]{null})));
        assertEquals(2, count(new ObjectVectorDirect<Integer>(5, null, 15)));
    }

    public void testCountDistinct() {
        assertEquals(NULL_LONG, countDistinct((ObjectVector<Short>)null));
        assertEquals(NULL_LONG, countDistinct((ObjectVector<Short>)null,true));
        assertEquals(0, countDistinct(new ObjectVectorDirect<Short>(new Short[]{})));
        assertEquals(0, countDistinct(new ObjectVectorDirect<Short>(new Short[]{NULL_SHORT})));
        assertEquals(1, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1})));
        assertEquals(2, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT})));
        assertEquals(2, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), false));
        assertEquals(3, countDistinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), true));
    }

    public void testDistinct() {
        assertEquals(null, distinct((ObjectVectorDirect<Short>)null));
        assertEquals(null, distinct((ObjectVectorDirect<Short>)null, true, true));
        assertEquals(new ObjectVectorDirect(), distinct(new ObjectVectorDirect<Short>(new Short[]{})));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{}), distinct(new ObjectVectorDirect<Short>(new Short[]{NULL_SHORT})));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{1}), distinct(new ObjectVectorDirect<Short>(new Short[]{1})));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{1,2}), distinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT})));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{1,2}), distinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), false, false));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{1,2,NULL_SHORT}), distinct(new ObjectVectorDirect<Short>(new Short[]{1,2,1,NULL_SHORT,NULL_SHORT}), true, false));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{1,2,3}), distinct(new ObjectVectorDirect<Short>(new Short[]{3,1,2,1,NULL_SHORT,NULL_SHORT}), false, true));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{1,2,3,4}), distinct(new ObjectVectorDirect<Short>(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}), false, true));
        assertEquals(new ObjectVectorDirect<Short>(new Short[]{NULL_SHORT,1,2,3,4}), distinct(new ObjectVectorDirect<Short>(new Short[]{3,1,2,4,1,NULL_SHORT,NULL_SHORT}), true, true));
    }

    public void testLast() {
        assertEquals(10, last(new ObjectVectorDirect<Object>(10)));
        assertEquals(3, last(new ObjectVectorDirect<Object>(1, 2, 3)));
        assertEquals(null, last(new ObjectVectorDirect<Object>(1, 2, null)));
    }

    public void testFirst() {
        assertEquals(10, first(new ObjectVectorDirect<Object>(10)));
        assertEquals(3, first(new ObjectVectorDirect<Object>(3, 2, 1)));
        assertEquals(null, first(new ObjectVectorDirect<Object>(null, 1, 2)));
    }

    public void testNth() {
        assertEquals(null, nth(-1, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(new Integer(40), nth(0, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(new Integer(50), nth(1, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(new Integer(60), nth(2, new ObjectVectorDirect<Integer>(40, 50, 60)));
        assertEquals(null, nth(10, new ObjectVectorDirect<Integer>(40, 50, 60)));
    }

    public void testVec() {
        assertEquals(new Character[]{new Character('1'), new Character('3'), new Character('5')}, vec(new ObjectVectorDirect<Character>(new Character('1'), new Character('3'), new Character('5'))));
    }

    public void testIn() {
        assertTrue(in(1000000, 1000000, 2000000, 3000000));
        assertFalse(in(5000000, 1000000, 2000000, 3000000));
        assertFalse(in(null, 1, 2, 3));
        assertTrue(in(null, 1, 2, null, 3));
    }

    public void testInRange() {
        assertTrue(inRange(2, 1, 3));
        assertTrue(inRange(1, 1, 3));
        assertFalse(inRange(null, 1, 3));
        assertTrue(inRange(3, 1, 3));
        assertFalse(inRange(4, 1, 3));
    }

    public void testMin() {
        assertEquals(new Integer(1), min(new ObjectVectorDirect<Integer>(new Integer(3), new Integer(1), new Integer(2))));
        assertEquals(new Integer(-2), min(new ObjectVectorDirect<Integer>(new Integer(3), new Integer(10), new Integer(-2), new Integer(1))));
        assertEquals(null, min(new ObjectVectorDirect<Integer>()));
    }

    public void testMax() {
        assertEquals(new Integer(3), max(new ObjectVectorDirect<Integer>(new Integer(3), new Integer(1), new Integer(2))));
        assertEquals(new Integer(10), max(new ObjectVectorDirect<Integer>(new Integer(3), new Integer(10), new Integer(-2), new Integer(1))));
        assertEquals(null, max(new ObjectVectorDirect<Integer>()));
    }

    public void testBinSearchIndex() {
        Short[] data = {1,3,4};
        assertEquals(NULL_INT, binSearchIndex(null, (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)0, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)1, BinSearch.BS_ANY));
        assertEquals(0, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)2, BinSearch.BS_ANY));
        assertEquals(1, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)3, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)4, BinSearch.BS_ANY));
        assertEquals(2, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)5, BinSearch.BS_ANY));
    }

    public void testRawBinSearchIndex() {
        assertEquals(QueryConstants.NULL_INT, rawBinSearchIndex(null, (short) 0, BinSearch.BS_ANY));
        assertEquals(QueryConstants.NULL_INT, rawBinSearchIndex(null, (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(QueryConstants.NULL_INT, rawBinSearchIndex(null, (short) 0, BinSearch.BS_LOWEST));

        Short[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(empty), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(empty), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(empty), (short) 0, BinSearch.BS_LOWEST));

        Short[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 12, BinSearch.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 12, BinSearch.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 12, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 11, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 11, BinSearch.BS_LOWEST));


        Short[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex(null, (short) 0, null);

        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 0, BinSearch.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 0, BinSearch.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 0, BinSearch.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 26, BinSearch.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 26, BinSearch.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 26, BinSearch.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 1, BinSearch.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 1, BinSearch.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 1, BinSearch.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 2, BinSearch.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 2, BinSearch.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 3, BinSearch.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 3, BinSearch.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 4, BinSearch.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 4, BinSearch.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 5, BinSearch.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 5, BinSearch.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 7, BinSearch.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 7, BinSearch.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 7, BinSearch.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 10, BinSearch.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 10, BinSearch.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 11, BinSearch.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 11, BinSearch.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 15, BinSearch.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 15, BinSearch.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 15, BinSearch.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 25, BinSearch.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 25, BinSearch.BS_LOWEST));
    }

    private class ComparableExtended implements Comparable<ComparableExtended> {

        private final Double i;

        private ComparableExtended(final Double i) {
            this.i = i;
        }

        @Override
        public int compareTo(@NotNull ComparableExtended o) {
            return i.compareTo(o.i);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ComparableExtended)) return false;

            ComparableExtended that = (ComparableExtended) o;

            return i != null ? i.equals(that.i) : that.i == null;
        }

        @Override
        public int hashCode() {
            return i != null ? i.hashCode() : 0;
        }
    }

    public void testMinVarArg() {
        final int size = 10;
        ComparableExtended[] ce = new ComparableExtended[size];

        final List<ComparableExtended> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final double value = Math.random() * 100;
            if (i % (size / 2) == 0) {
                ce[i] = null;
            } else {
                ce[i] = new ComparableExtended(value);
                list.add(new ComparableExtended(value));
            }
        }

        assertEquals(Collections.min(list), min(ce));

        ce = null;
        assertNull(min(ce));

        ce = new ComparableExtended[]{};
        assertNull(min(ce));

        final Byte[] bytes = new Byte[]{1, 3, 9, io.deephaven.util.QueryConstants.NULL_BYTE, null, 78};
        assertEquals(Byte.valueOf((byte) 1), min(bytes));

        final Short[] shorts = new Short[]{1, 3, 9, io.deephaven.util.QueryConstants.NULL_SHORT, null, 78};
        assertEquals(Short.valueOf((short) 1), min(shorts));

        final Integer[] ints = new Integer[]{1, 3, 9, io.deephaven.util.QueryConstants.NULL_INT, null, 78};
        assertEquals(Integer.valueOf(1), min(ints));

        final Float[] floats = new Float[]{1f, 3f, 9f, io.deephaven.util.QueryConstants.NULL_FLOAT, null, 78f};
        assertEquals(Float.valueOf(1f), min(floats));

        final Long[] longs = new Long[]{1l, 3l, 9l, io.deephaven.util.QueryConstants.NULL_LONG, null, 78l};
        assertEquals(Long.valueOf((long) 1), min(longs));

        final Double[] doubles = new Double[]{1d, 3d, 9d, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, 78d};
        assertEquals(Double.valueOf((double) 1), min(doubles));

        assertEquals(-9f, min(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, io.deephaven.util
                .QueryConstants.NULL_INT, Double.NaN));

        assertEquals(-9d, min(new Object[]{1d, 2d, -3d, -9d, 7d, null, io.deephaven.util
                .QueryConstants.NULL_DOUBLE, Double.NaN}));

        assertEquals((long) io.deephaven.util.QueryConstants.NULL_INT, min(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, (long) io.deephaven.util.QueryConstants.NULL_INT));
    }

    public void testMinVarArgsException() {
        try {
            min("A", 2);
            fail("Testcase should fail for min(\"A\", 2)");
        } catch (final IllegalArgumentException cce) {
            assertTrue(cce.getMessage().contains("Can not compare"));
        }
    }

    public void testMaxVarArgsException() {
        try {
            max("A", 2);
            fail("Testcase should fail for max(\"A\", 2)");
        } catch (final IllegalArgumentException cce) {
            assertTrue(cce.getMessage().contains("Can not compare"));
        }
    }

    public void testMaxVarArg() {
        final int size = 10;
        ComparableExtended[] ce = new ComparableExtended[size];

        final List<ComparableExtended> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final double value = Math.random() * 100;
            if (i % (size / 2) == 0) {
                ce[i] = null;
            } else {
                ce[i] = new ComparableExtended(value);
                list.add(new ComparableExtended(value));
            }
        }

        assertEquals(Collections.max(list), max(ce));

        ce = null;
        assertNull(max(ce));

        ce = new ComparableExtended[]{};
        assertNull(max(ce));

        final Byte[] bytes = new Byte[]{1, 3, 9, io.deephaven.util.QueryConstants.NULL_BYTE, null, 78};
        assertEquals(Byte.valueOf((byte) 78), max(bytes));

        final Short[] shorts = new Short[]{1, 3, 9, io.deephaven.util.QueryConstants.NULL_SHORT, null, 78};
        assertEquals(Short.valueOf((short) 78), max(shorts));

        final Integer[] ints = new Integer[]{1, 3, 9, io.deephaven.util.QueryConstants.NULL_INT, null, 78};
        assertEquals(Integer.valueOf(78), max(ints));

        final Float[] floats = new Float[]{1f, 3f, 9f, io.deephaven.util.QueryConstants.NULL_FLOAT, null, 78f};
        assertEquals(Float.valueOf(78), max(floats));

        final Long[] longs = new Long[]{1l, 3l, 9l, io.deephaven.util.QueryConstants.NULL_LONG, null, 78l};
        assertEquals(Long.valueOf((long) 78), max(longs));

        final Double[] doubles = new Double[]{1d, 3d, 9d, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, 78d};
        assertEquals(Double.valueOf((double) 78), max(doubles));

        assertEquals((byte) 8, max(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, io.deephaven.util
                .QueryConstants.NULL_INT, Double.NaN));

        assertEquals(7d, max(new Object[]{1d, 2d, -3d, -9d, 7d, null, io.deephaven.util
                .QueryConstants.NULL_DOUBLE, Double.NaN}));

        assertEquals(new BigDecimal("32.6598745"), max(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, (long) io.deephaven.util.QueryConstants.NULL_INT, new BigDecimal("32.6598745")));
    }

    public void testSort() {
        final ObjectVector<ComparableExtended> comparableExtendedVector = new ObjectVectorDirect<>(null, new ComparableExtended(1d), new ComparableExtended(12d), new ComparableExtended(0d)
                , new ComparableExtended(9.4d), null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d));
        ObjectVector<ComparableExtended> sort = sort(comparableExtendedVector);
        ObjectVector<ComparableExtended> expected = new ObjectVectorDirect<>(null, null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d),
                new ComparableExtended(0d), new ComparableExtended(1d), new ComparableExtended(9.4d), new ComparableExtended(12d));
        assertEquals(expected, sort);

        final BigDecimal[] bigDecimals = new BigDecimal[]{null, new BigDecimal("2.369"), new BigDecimal(26), new BigDecimal(Float.MAX_VALUE), null, new BigDecimal(Long.MIN_VALUE)
                , new BigDecimal(-2395.365)};
        BigDecimal[] expectedBDs = new BigDecimal[]{null, null, new BigDecimal(Long.MIN_VALUE), new BigDecimal(-2395.365), new BigDecimal("2.369"), new BigDecimal(26), new BigDecimal(Float.MAX_VALUE)
        };
        BigDecimal[] sortedArray = sort(bigDecimals);
        assertEquals(expectedBDs, sortedArray);

        final Byte[] bytes = new Byte[]{null, (byte) 1, (byte) 9, (byte) 3, io.deephaven.util.QueryConstants.NULL_BYTE, null, (byte) 78, io.deephaven.util.QueryConstants
                .NULL_BYTE};
        final Byte[] sortBytesArray = sort(bytes);
        final Byte[] expectedSortedByteArray = new Byte[]{null, io.deephaven.util.QueryConstants.NULL_BYTE, null, io.deephaven.util.QueryConstants.NULL_BYTE, (byte) 1, (byte) 3, (byte) 9, (byte) 78};
        assertEquals(expectedSortedByteArray, sortBytesArray);

        final ObjectVector<Byte> byteVector = new ObjectVectorDirect<>(null, (byte) 1, (byte) 9, (byte) 3, io.deephaven.util.QueryConstants.NULL_BYTE, null, (byte) 78, io.deephaven.util.QueryConstants
                .NULL_BYTE);
        final ObjectVector<Byte> sortedByteVector = sort(byteVector);
        final ObjectVector<Byte> expectedSortedByteVector = new ObjectVectorDirect<>(null, io.deephaven.util.QueryConstants.NULL_BYTE, null, io.deephaven.util
                .QueryConstants.NULL_BYTE, (byte) 1, (byte) 3, (byte) 9, (byte) 78);
        assertEquals(expectedSortedByteVector, sortedByteVector);

        final Short[] shorts = new Short[]{null, (short) 1, (short) 9, (short) 3, io.deephaven.util.QueryConstants.NULL_SHORT, null, (short) 78, io.deephaven.util.QueryConstants
                .NULL_SHORT};
        final Short[] sortShortsArray = sort(shorts);
        final Short[] expectedSortedShortArray = new Short[]{null, io.deephaven.util.QueryConstants.NULL_SHORT, null, io.deephaven.util.QueryConstants
                .NULL_SHORT, (short) 1, (short) 3, (short) 9, (short) 78,};
        assertEquals(expectedSortedShortArray, sortShortsArray);

        final ObjectVector<Short> shortVector = new ObjectVectorDirect<>(null, (short) 1, (short) 9, (short) 3, io.deephaven.util.QueryConstants.NULL_SHORT, null, (short) 78, io.deephaven.util.QueryConstants
                .NULL_SHORT);
        final ObjectVector<Short> sortedShortVector = sort(shortVector);
        final ObjectVector<Short> expectedSortedShortVector = new ObjectVectorDirect<>(null, io.deephaven.util.QueryConstants.NULL_SHORT, null, io.deephaven.util
                .QueryConstants.NULL_SHORT, (short) 1, (short) 3, (short) 9, (short) 78);
        assertEquals(expectedSortedShortVector, sortedShortVector);

        final Integer[] ints = new Integer[]{null, (int) 1, (int) 9, (int) 3, io.deephaven.util.QueryConstants.NULL_INT, null, (int) 78, io.deephaven.util.QueryConstants.NULL_INT};
        final Integer[] sortIntegersArray = sort(ints);
        final Integer[] expectedSortedIntegerArray = new Integer[]{null, io.deephaven.util.QueryConstants.NULL_INT, null, io.deephaven.util.QueryConstants.NULL_INT, (int) 1, (int) 3, (int) 9, (int)
                78,};
        assertEquals(expectedSortedIntegerArray, sortIntegersArray);

        final ObjectVector<Integer> intVector = new ObjectVectorDirect<>(null, (int) 1, (int) 9, (int) 3, io.deephaven.util.QueryConstants.NULL_INT, null, (int) 78, io.deephaven.util.QueryConstants
                .NULL_INT);
        final ObjectVector<Integer> sortedIntegerVector = sort(intVector);
        final ObjectVector<Integer> expectedSortedIntegerVector = new ObjectVectorDirect<>(null, io.deephaven.util.QueryConstants.NULL_INT, null, io.deephaven.util
                .QueryConstants.NULL_INT, (int) 1, (int) 3, (int) 9, (int) 78);
        assertEquals(expectedSortedIntegerVector, sortedIntegerVector);

        final Float[] floats = new Float[]{null, (float) 1, (float) 9, (float) 3, io.deephaven.util.QueryConstants.NULL_FLOAT, null, (float) 78, io.deephaven.util.QueryConstants
                .NULL_FLOAT};
        final Float[] sortFloatsArray = sort(floats);
        final Float[] expectedSortedFloatArray = new Float[]{null, io.deephaven.util.QueryConstants.NULL_FLOAT, null, io.deephaven.util.QueryConstants
                .NULL_FLOAT, (float) 1, (float) 3, (float) 9, (float) 78};
        assertEquals(expectedSortedFloatArray, sortFloatsArray);

        final ObjectVector<Float> floatVector = new ObjectVectorDirect<>(null, (float) 1, (float) 9, (float) 3, io.deephaven.util.QueryConstants.NULL_FLOAT, null, (float) 78, io.deephaven.util.QueryConstants
                .NULL_FLOAT);
        final ObjectVector<Float> sortedFloatVector = sort(floatVector);
        final ObjectVector<Float> expectedSortedFloatVector = new ObjectVectorDirect<>(null, io.deephaven.util.QueryConstants.NULL_FLOAT, null, io.deephaven.util
                .QueryConstants.NULL_FLOAT, (float) 1, (float) 3, (float) 9, (float) 78);
        assertEquals(expectedSortedFloatVector, sortedFloatVector);

        final Long[] longs = new Long[]{null, (long) 1, (long) 9, (long) 3, io.deephaven.util.QueryConstants.NULL_LONG, null, (long) 78, io.deephaven.util.QueryConstants
                .NULL_LONG};
        final Long[] sortLongsArray = sort(longs);
        final Long[] expectedSortedLongArray = new Long[]{null, io.deephaven.util.QueryConstants.NULL_LONG, null, io.deephaven.util.QueryConstants.NULL_LONG, (long) 1, (long) 3, (long) 9, (long) 78};
        assertEquals(expectedSortedLongArray, sortLongsArray);

        final ObjectVector<Long> longVector = new ObjectVectorDirect<>(null, (long) 1, (long) 9, (long) 3, io.deephaven.util.QueryConstants.NULL_LONG, null, (long) 78, io.deephaven.util.QueryConstants
                .NULL_LONG);
        final ObjectVector<Long> sortedLongVector = sort(longVector);
        final ObjectVector<Long> expectedSortedLongVector = new ObjectVectorDirect<>(null, io.deephaven.util.QueryConstants.NULL_LONG, null, io.deephaven.util
                .QueryConstants.NULL_LONG, (long) 1, (long) 3, (long) 9, (long) 78);
        assertEquals(expectedSortedLongVector, sortedLongVector);

        final Double[] doubles = new Double[]{null, (double) 1, (double) 9, (double) 3, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, (double) 78, io.deephaven.util.QueryConstants
                .NULL_DOUBLE};
        final Double[] sortDoublesArray = sort(doubles);
        final Double[] expectedSortedDoubleArray = new Double[]{null, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, io.deephaven.util
                .QueryConstants.NULL_DOUBLE, (double) 1, (double) 3, (double) 9, (double) 78};
        assertEquals(expectedSortedDoubleArray, sortDoublesArray);

        final ObjectVector<Double> doubleVector = new ObjectVectorDirect<>(null, (double) 1, (double) 9, (double) 3, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, (double) 78, io.deephaven.util.QueryConstants
                .NULL_DOUBLE, Double.NaN);
        final ObjectVector<Double> sortedDoubleVector = sort(doubleVector);
        final ObjectVector<Double> expectedSortedDoubleVector = new ObjectVectorDirect<>(null, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, io.deephaven
                .util.QueryConstants.NULL_DOUBLE, (double) 1, (double) 3, (double) 9, (double) 78, Double.NaN);
        assertEquals(expectedSortedDoubleVector, sortedDoubleVector);

        assertEquals(new Number[]{null, io.deephaven.util.QueryConstants.NULL_INT, -9f, -3l, 1, 2d, (short) 3, 7d, (byte) 8}, sort(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, io.deephaven.util
                .QueryConstants.NULL_INT));

        assertEquals(new Number[]{null, io.deephaven.util.QueryConstants.NULL_INT, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, -9f, -3l, 1, 2d, (short) 3, 7d, (byte) 8, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, Double.NaN},
                sort(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, io.deephaven.util.QueryConstants.NULL_INT, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));

        assertEquals(new Number[]{io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG, Long.MIN_VALUE, null, Double.NEGATIVE_INFINITY, -5.3d, -5l, -5d, Double.MIN_VALUE, 123l, 123d,
                        123l, 234d, 1.23456789E9, 1234567891L, Long.MAX_VALUE, new Double("1.347587244542345673435434E20"), Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN},
                sort(123l, 123d, 123l, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG, 234d, Long
                        .MAX_VALUE, Long.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, 1.23456789E9, 1234567891L, new Double("1.347587244542345673435434E20"), null, -5l, -5.3d, -5d));

        assertEquals(new Object[]{io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG, Long.MIN_VALUE, null, Double.NEGATIVE_INFINITY, -5.3d, -5l, -5d, Double
                        .MIN_VALUE, 123l, 123d,
                        123l, 234d, 1.23456789E9, 1234567891L, Long.MAX_VALUE, new Double("1.347587244542345673435434E20"), Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN},
                sort(new Object[]{123l, 123d, 123l, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants
                        .NULL_LONG, 234d, Long.MAX_VALUE, Long.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, 1.23456789E9, 1234567891L, new Double("1.347587244542345673435434E20"), null, -5l,
                        -5.3d, -5d}));
    }

    public void testSortDescending() {
        final ObjectVector<ComparableExtended> comparableExtendedVector = new ObjectVectorDirect<>(null, new ComparableExtended(1d), new ComparableExtended(12d), new ComparableExtended(0d)
                , new ComparableExtended(9.4d), null, new ComparableExtended(-5.6d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d));

        ObjectVector<ComparableExtended> sort = sortDescending(comparableExtendedVector);
        ObjectVector<ComparableExtended> expected = new ObjectVectorDirect<>(new ComparableExtended(12d), new ComparableExtended(9.4d), new ComparableExtended(1d),
                new ComparableExtended(0d), new ComparableExtended(-2.3d), new ComparableExtended(-2.3d), new ComparableExtended(-5.6d), null, null);
        assertEquals(expected, sort);

        final BigDecimal[] bigDecimals = new BigDecimal[]{null, new BigDecimal("2.369"), new BigDecimal(26), new BigDecimal(Float.MAX_VALUE), null, new BigDecimal(Long.MIN_VALUE)
                , new BigDecimal(-2395.365)};
        BigDecimal[] expectedBDs = new BigDecimal[]{new BigDecimal(Float.MAX_VALUE), new BigDecimal(26), new BigDecimal("2.369"), new BigDecimal(-2395.365), new BigDecimal(Long.MIN_VALUE), null, null};
        BigDecimal[] sortedArray = sortDescending(bigDecimals);
        assertEquals(expectedBDs, sortedArray);

        final Byte[] bytes = new Byte[]{null, (byte) 1, (byte) 9, (byte) 3, io.deephaven.util.QueryConstants.NULL_BYTE, null, (byte) 78, io.deephaven.util.QueryConstants
                .NULL_BYTE};
        final Byte[] sortedBytesArray = sortDescending(bytes);
        final Byte[] expectedSortedByteArray = new Byte[]{(byte) 78, (byte) 9, (byte) 3, (byte) 1, null, io.deephaven.util.QueryConstants.NULL_BYTE, null, io.deephaven.util.QueryConstants
                .NULL_BYTE};
        assertEquals(expectedSortedByteArray, sortedBytesArray);

        final ObjectVector<Byte> byteVector = new ObjectVectorDirect<>(null, (byte) 1, (byte) 9, (byte) 3, io.deephaven.util.QueryConstants.NULL_BYTE, null, (byte) 78, io.deephaven.util.QueryConstants
                .NULL_BYTE);
        ObjectVector<Byte> sortedBytesVector = sortDescending(byteVector);
        ObjectVector<Byte> expectedSortedBytesVector = new ObjectVectorDirect<>((byte) 78, (byte) 9, (byte) 3, (byte) 1, null, io.deephaven.util.QueryConstants.NULL_BYTE, null, io.deephaven.util.QueryConstants
                .NULL_BYTE);
        assertEquals(expectedSortedBytesVector, sortedBytesVector);

        final Short[] shorts = new Short[]{null, (short) 1, (short) 9, (short) 3, io.deephaven.util.QueryConstants.NULL_SHORT, null, (short) 78, io.deephaven.util.QueryConstants
                .NULL_SHORT};
        final Short[] sortedShortsArray = sortDescending(shorts);
        final Short[] expectedSortedShortArray = new Short[]{(short) 78, (short) 9, (short) 3, (short) 1, null, io.deephaven.util.QueryConstants.NULL_SHORT, null, io.deephaven.util.QueryConstants
                .NULL_SHORT};
        assertEquals(expectedSortedShortArray, sortedShortsArray);

        final ObjectVector<Short> shortVector = new ObjectVectorDirect<>(null, (short) 1, (short) 9, (short) 3, io.deephaven.util.QueryConstants.NULL_SHORT, null, (short) 78, io.deephaven.util.QueryConstants
                .NULL_SHORT);
        ObjectVector<Short> sortedShortsVector = sortDescending(shortVector);
        ObjectVector<Short> expectedSortedShortsVector = new ObjectVectorDirect<>((short) 78, (short) 9, (short) 3, (short) 1, null, io.deephaven.util.QueryConstants.NULL_SHORT, null, io.deephaven.util
                .QueryConstants
                .NULL_SHORT);
        assertEquals(expectedSortedShortsVector, sortedShortsVector);

        final Integer[] ints = new Integer[]{null, (int) 1, (int) 9, (int) 3, io.deephaven.util.QueryConstants.NULL_INT, null, (int) 78, io.deephaven.util.QueryConstants
                .NULL_INT};
        final Integer[] sortedIntegersArray = sortDescending(ints);
        final Integer[] expectedSortedIntegerArray = new Integer[]{(int) 78, (int) 9, (int) 3, (int) 1, null, io.deephaven.util.QueryConstants.NULL_INT, null, io.deephaven.util.QueryConstants
                .NULL_INT};
        assertEquals(expectedSortedIntegerArray, sortedIntegersArray);

        final ObjectVector<Integer> intVector = new ObjectVectorDirect<>(null, (int) 1, (int) 9, (int) 3, io.deephaven.util.QueryConstants.NULL_INT, null, (int) 78, io.deephaven.util.QueryConstants
                .NULL_INT);
        ObjectVector<Integer> sortedIntegersVector = sortDescending(intVector);
        ObjectVector<Integer> expectedSortedIntegersVector = new ObjectVectorDirect<>((int) 78, (int) 9, (int) 3, (int) 1, null, io.deephaven.util.QueryConstants.NULL_INT, null, io.deephaven.util.QueryConstants
                .NULL_INT);
        assertEquals(expectedSortedIntegersVector, sortedIntegersVector);

        final Float[] floats = new Float[]{null, (float) 1, (float) 9, (float) 3, io.deephaven.util.QueryConstants.NULL_FLOAT, null, (float) 78, io.deephaven.util.QueryConstants
                .NULL_FLOAT};
        final Float[] sortedFloatsArray = sortDescending(floats);
        final Float[] expectedSortedFloatArray = new Float[]{(float) 78, (float) 9, (float) 3, (float) 1, null, io.deephaven.util.QueryConstants.NULL_FLOAT, null, io.deephaven.util.QueryConstants
                .NULL_FLOAT};
        assertEquals(expectedSortedFloatArray, sortedFloatsArray);

        final ObjectVector<Float> floatVector = new ObjectVectorDirect<>(null, (float) 1, (float) 9, (float) 3, io.deephaven.util.QueryConstants.NULL_FLOAT, null, (float) 78, io.deephaven.util.QueryConstants
                .NULL_FLOAT);
        ObjectVector<Float> sortedFloatsVector = sortDescending(floatVector);
        ObjectVector<Float> expectedSortedFloatsVector = new ObjectVectorDirect<>((float) 78, (float) 9, (float) 3, (float) 1, null, io.deephaven.util.QueryConstants.NULL_FLOAT, null, io.deephaven.util
                .QueryConstants
                .NULL_FLOAT);
        assertEquals(expectedSortedFloatsVector, sortedFloatsVector);

        final Long[] longs = new Long[]{null, (long) 1, (long) 9, (long) 3, io.deephaven.util.QueryConstants.NULL_LONG, null, (long) 78, io.deephaven.util.QueryConstants
                .NULL_LONG};
        final Long[] sortedLongsArray = sortDescending(longs);
        final Long[] expectedSortedLongArray = new Long[]{(long) 78, (long) 9, (long) 3, (long) 1, null, io.deephaven.util.QueryConstants.NULL_LONG, null, io.deephaven.util.QueryConstants
                .NULL_LONG};
        assertEquals(expectedSortedLongArray, sortedLongsArray);

        final ObjectVector<Long> longVector = new ObjectVectorDirect<>(null, (long) 1, (long) 9, (long) 3, io.deephaven.util.QueryConstants.NULL_LONG, null, (long) 78, io.deephaven.util.QueryConstants
                .NULL_LONG);
        ObjectVector<Long> sortedLongsVector = sortDescending(longVector);
        ObjectVector<Long> expectedSortedLongsVector = new ObjectVectorDirect<>((long) 78, (long) 9, (long) 3, (long) 1, null, io.deephaven.util.QueryConstants.NULL_LONG, null, io.deephaven.util.QueryConstants
                .NULL_LONG);
        assertEquals(expectedSortedLongsVector, sortedLongsVector);

        final Double[] doubles = new Double[]{null, (double) 1, (double) 9, (double) 3, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, (double) 78, io.deephaven.util.QueryConstants
                .NULL_DOUBLE};
        final Double[] sortedDoublesArray = sortDescending(doubles);
        final Double[] expectedSortedDoubleArray = new Double[]{(double) 78, (double) 9, (double) 3, (double) 1, null, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, io.deephaven.util
                .QueryConstants.NULL_DOUBLE};
        assertEquals(expectedSortedDoubleArray, sortedDoublesArray);

        final ObjectVector<Double> doubleVector = new ObjectVectorDirect<>(null, (double) 1, (double) 9, (double) 3, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, (double) 78, io.deephaven
                .util.QueryConstants.NULL_DOUBLE, Double.NaN);
        ObjectVector<Double> sortedDoublesVector = sortDescending(doubleVector);
        ObjectVector<Double> expectedSortedDoublesVector = new ObjectVectorDirect<>(Double.NaN, (double) 78, (double) 9, (double) 3, (double) 1, null, io.deephaven.util.QueryConstants.NULL_DOUBLE, null, io.deephaven.util
                .QueryConstants.NULL_DOUBLE);
        assertEquals(expectedSortedDoublesVector, sortedDoublesVector);

        assertEquals(new Number[]{(byte) 8, 7d, (short) 3, 2d, 1, -3l, -9f, null, io.deephaven.util.QueryConstants.NULL_INT}, sortDescending(1, 2d, -3l, -9f, 7d,
                (byte) 8, (short) 3, null, io.deephaven.util.QueryConstants.NULL_INT));

        assertEquals(new Number[]{Double.NaN, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, (byte) 8, 7d, (short) 3, 2d, 1, -3l, -9f, Double
                        .NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, null, io.deephaven.util.QueryConstants.NULL_INT},
                sortDescending(1, 2d, -3l, -9f, 7d, (byte) 8, (short) 3, null, io.deephaven.util.QueryConstants.NULL_INT, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float
                        .POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));

        assertEquals(new Number[]{Double.NaN, Double.POSITIVE_INFINITY, Double.MAX_VALUE, new Double("1.347587244542345673435434E20"), Long.MAX_VALUE, 1234567891L, 1.23456789E9,
                        234d, 123l, 123d, 123l, Double.MIN_VALUE, -5l, -5d, -5.3d, Double.NEGATIVE_INFINITY, io.deephaven.util.QueryConstants.NULL_LONG, io.deephaven.util.QueryConstants.NULL_DOUBLE, Long.MIN_VALUE, null},
                sortDescending(123l, 123d, 123l, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, io.deephaven.util.QueryConstants.NULL_LONG,
                        234d, Long.MAX_VALUE, io.deephaven.util.QueryConstants.NULL_DOUBLE, Long.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, 1.23456789E9, 1234567891L, new Double
                                ("1.347587244542345673435434E20"), null, -5l, -5.3d, -5d));

        assertEquals(new Object[]{Double.NaN, Double.POSITIVE_INFINITY, Double.MAX_VALUE, new Double("1.347587244542345673435434E20"), Long.MAX_VALUE, 1234567891L, 1.23456789E9,
                        234d, 123l, 123d, 123l, Double.MIN_VALUE, -5l, -5d, -5.3d, Double.NEGATIVE_INFINITY, io.deephaven.util.QueryConstants.NULL_LONG, io.deephaven.util.QueryConstants.NULL_DOUBLE, Long.MIN_VALUE, null},
                sortDescending(new Object[]{123l, 123d, 123l, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, io.deephaven.util.QueryConstants.NULL_LONG,
                        234d, Long.MAX_VALUE, io.deephaven.util.QueryConstants.NULL_DOUBLE, Long.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, 1.23456789E9, 1234567891L, new Double
                        ("1.347587244542345673435434E20"), null, -5l, -5.3d, -5d}));

    }

    public void testSortExceptions() {
        //sort
        ObjectVector vectorToSort = null;

        ObjectVector sort = sort(vectorToSort);
        assertNull(sort);

        BigDecimal[] bd = null;
        BigDecimal[] sortedNumbers = sort(bd);
        assertNull(sortedNumbers);

        sort = sort(new ObjectVectorDirect<ComparableExtended>());
        assertEquals(new ObjectVectorDirect<ComparableExtended>(), sort);

        bd = new BigDecimal[]{};
        sortedNumbers = sort(bd);
        assertTrue(ArrayUtils.isEmpty(sortedNumbers));

        try {
            sort("A", 2);
            fail("Testcase should fail for sort(\"A\", 2)");
        } catch (final RuntimeException cce) {
            assertTrue(cce.getMessage().contains("Can not compare classes"));
        }

        //sortDescending

        sort = sortDescending(vectorToSort);
        assertNull(sort);

        bd = null;
        sortedNumbers = sortDescending(bd);
        assertNull(sortedNumbers);

        sort = sortDescending(new ObjectVectorDirect<ComparableExtended>());
        assertEquals(new ObjectVectorDirect<ComparableExtended>(), sort);

        bd = new BigDecimal[]{};
        sortedNumbers = sortDescending(bd);
        assertTrue(ArrayUtils.isEmpty(sortedNumbers));

        try {
            sortDescending("A", 2);
            fail("Testcase should fail for sortDescending(\"A\", 2)");
        } catch (final RuntimeException cce) {
            assertTrue(cce.getMessage().contains("Can not compare classes"));
        }
    }
}
