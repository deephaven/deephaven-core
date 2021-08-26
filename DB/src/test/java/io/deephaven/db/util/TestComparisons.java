package io.deephaven.db.util;

import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

public class TestComparisons {
    @Test
    public void testCharCharComparisons() {
        TestCase.assertTrue(DhCharComparisons.lt(QueryConstants.NULL_CHAR, 'A'));
        TestCase.assertTrue(DhCharComparisons.gt('A', QueryConstants.NULL_CHAR));
        TestCase
            .assertTrue(DhCharComparisons.eq(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertTrue(DhCharComparisons.lt('A', 'B'));
        TestCase.assertFalse(DhCharComparisons.gt('A', 'B'));
        TestCase
            .assertTrue(DhCharComparisons.eq(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertFalse(DhCharComparisons.eq('A', QueryConstants.NULL_CHAR));
    }

    @Test
    public void testByteByteComparisons() {
        TestCase.assertTrue(DhByteComparisons.lt(QueryConstants.NULL_BYTE, (byte) 2));
        TestCase.assertTrue(DhByteComparisons.gt((byte) 2, QueryConstants.NULL_BYTE));
        TestCase
            .assertTrue(DhByteComparisons.eq(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertTrue(DhByteComparisons.lt((byte) 2, (byte) 3));
        TestCase.assertFalse(DhByteComparisons.gt((byte) 2, (byte) 3));
        TestCase
            .assertTrue(DhByteComparisons.eq(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertFalse(DhByteComparisons.eq((byte) 2, QueryConstants.NULL_BYTE));
    }

    @Test
    public void testShortShortComparisons() {
        TestCase.assertTrue(DhShortComparisons.lt(QueryConstants.NULL_SHORT, (short) 2));
        TestCase.assertTrue(DhShortComparisons.gt((short) 2, QueryConstants.NULL_SHORT));
        TestCase.assertTrue(
            DhShortComparisons.eq(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertTrue(DhShortComparisons.lt((short) 2, (short) 3));
        TestCase.assertFalse(DhShortComparisons.gt((short) 2, (short) 3));
        TestCase.assertTrue(
            DhShortComparisons.eq(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertFalse(DhShortComparisons.eq((short) 2, QueryConstants.NULL_SHORT));
    }

    @Test
    public void testIntIntComparisons() {
        TestCase.assertTrue(DhIntComparisons.lt(QueryConstants.NULL_INT, (int) 2));
        TestCase.assertTrue(DhIntComparisons.gt(2, QueryConstants.NULL_INT));
        TestCase.assertTrue(DhIntComparisons.eq(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertTrue(DhIntComparisons.lt(2, 3));
        TestCase.assertFalse(DhIntComparisons.gt(2, 3));
        TestCase.assertTrue(DhIntComparisons.eq(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertFalse(DhIntComparisons.eq(2, QueryConstants.NULL_INT));
    }

    @Test
    public void testLongLongComparisons() {
        TestCase.assertTrue(DhLongComparisons.lt(QueryConstants.NULL_LONG, 2));
        TestCase.assertTrue(DhLongComparisons.gt(2, QueryConstants.NULL_LONG));
        TestCase
            .assertTrue(DhLongComparisons.eq(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertTrue(DhLongComparisons.lt(2, 3));
        TestCase.assertFalse(DhLongComparisons.gt(2, 3));
        TestCase
            .assertTrue(DhLongComparisons.eq(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertFalse(DhLongComparisons.eq(2, QueryConstants.NULL_LONG));
    }

    @Test
    public void testFloatFloatComparisons() {
        TestCase.assertTrue(DhFloatComparisons.lt(QueryConstants.NULL_FLOAT, 1.0f));
        TestCase.assertTrue(DhFloatComparisons.gt(2.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(
            DhFloatComparisons.eq(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(DhFloatComparisons.lt(2.0f, 100f));
        TestCase.assertFalse(DhFloatComparisons.gt(2.0f, 100f));
        TestCase.assertTrue(
            DhFloatComparisons.eq(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(DhFloatComparisons.eq(Float.NaN, Float.NaN));
        TestCase.assertTrue(DhFloatComparisons.lt(QueryConstants.NULL_FLOAT, Float.NaN));
        TestCase.assertTrue(DhFloatComparisons.lt(1, Float.NaN));
        TestCase
            .assertTrue(DhFloatComparisons.eq(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase
            .assertTrue(DhFloatComparisons.eq(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(DhFloatComparisons.gt(Float.POSITIVE_INFINITY, 7f));
        TestCase.assertTrue(DhFloatComparisons.lt(Float.NEGATIVE_INFINITY, 7f));
        TestCase.assertTrue(DhFloatComparisons.lt(7f, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(DhFloatComparisons.gt(7f, Float.NEGATIVE_INFINITY));
        TestCase.assertEquals(0, DhFloatComparisons.compare(0f, -0.0f));
        TestCase.assertEquals(0, DhFloatComparisons.compare(-0.0f, 0.0f));
        TestCase
            .assertTrue(DhFloatComparisons.lt(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase
            .assertTrue(DhFloatComparisons.gt(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertEquals(0,
            DhFloatComparisons.compare(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertEquals(0,
            DhFloatComparisons.compare(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(DhFloatComparisons.gt(Float.NaN, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(DhFloatComparisons.lt(Float.POSITIVE_INFINITY, Float.NaN));
    }
}
