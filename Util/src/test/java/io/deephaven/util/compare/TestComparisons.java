package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

public class TestComparisons {

    @Test
    public void testCharCharComparisons() {
        TestCase.assertTrue(CharComparisons.lt(QueryConstants.NULL_CHAR, 'A'));
        TestCase.assertTrue(CharComparisons.gt('A', QueryConstants.NULL_CHAR));
        TestCase.assertTrue(CharComparisons.eq(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertTrue(CharComparisons.lt('A', 'B'));
        TestCase.assertFalse(CharComparisons.gt('A', 'B'));
        TestCase.assertTrue(CharComparisons.eq(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertFalse(CharComparisons.eq('A', QueryConstants.NULL_CHAR));
    }

    @Test
    public void testByteByteComparisons() {
        TestCase.assertTrue(ByteComparisons.lt(QueryConstants.NULL_BYTE, (byte) 2));
        TestCase.assertTrue(ByteComparisons.gt((byte) 2, QueryConstants.NULL_BYTE));
        TestCase.assertTrue(ByteComparisons.eq(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertTrue(ByteComparisons.lt((byte) 2, (byte) 3));
        TestCase.assertFalse(ByteComparisons.gt((byte) 2, (byte) 3));
        TestCase.assertTrue(ByteComparisons.eq(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertFalse(ByteComparisons.eq((byte) 2, QueryConstants.NULL_BYTE));
    }

    @Test
    public void testShortShortComparisons() {
        TestCase.assertTrue(ShortComparisons.lt(QueryConstants.NULL_SHORT, (short) 2));
        TestCase.assertTrue(ShortComparisons.gt((short) 2, QueryConstants.NULL_SHORT));
        TestCase.assertTrue(ShortComparisons.eq(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertTrue(ShortComparisons.lt((short) 2, (short) 3));
        TestCase.assertFalse(ShortComparisons.gt((short) 2, (short) 3));
        TestCase.assertTrue(ShortComparisons.eq(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertFalse(ShortComparisons.eq((short) 2, QueryConstants.NULL_SHORT));
    }

    @Test
    public void testIntIntComparisons() {
        TestCase.assertTrue(IntComparisons.lt(QueryConstants.NULL_INT, (int) 2));
        TestCase.assertTrue(IntComparisons.gt(2, QueryConstants.NULL_INT));
        TestCase.assertTrue(IntComparisons.eq(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertTrue(IntComparisons.lt(2, 3));
        TestCase.assertFalse(IntComparisons.gt(2, 3));
        TestCase.assertTrue(IntComparisons.eq(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertFalse(IntComparisons.eq(2, QueryConstants.NULL_INT));
    }

    @Test
    public void testLongLongComparisons() {
        TestCase.assertTrue(LongComparisons.lt(QueryConstants.NULL_LONG, 2));
        TestCase.assertTrue(LongComparisons.gt(2, QueryConstants.NULL_LONG));
        TestCase.assertTrue(LongComparisons.eq(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertTrue(LongComparisons.lt(2, 3));
        TestCase.assertFalse(LongComparisons.gt(2, 3));
        TestCase.assertTrue(LongComparisons.eq(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertFalse(LongComparisons.eq(2, QueryConstants.NULL_LONG));
    }

    @Test
    public void testFloatFloatComparisons() {
        TestCase.assertTrue(FloatComparisons.lt(QueryConstants.NULL_FLOAT, 1.0f));
        TestCase.assertTrue(FloatComparisons.gt(2.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatComparisons.eq(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatComparisons.lt(2.0f, 100f));
        TestCase.assertFalse(FloatComparisons.gt(2.0f, 100f));
        TestCase.assertTrue(FloatComparisons.eq(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatComparisons.eq(Float.NaN, Float.NaN));
        TestCase.assertTrue(FloatComparisons.lt(QueryConstants.NULL_FLOAT, Float.NaN));
        TestCase.assertTrue(FloatComparisons.lt(1, Float.NaN));
        TestCase.assertTrue(FloatComparisons.eq(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.eq(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.POSITIVE_INFINITY, 7f));
        TestCase.assertTrue(FloatComparisons.lt(Float.NEGATIVE_INFINITY, 7f));
        TestCase.assertTrue(FloatComparisons.lt(7f, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(7f, Float.NEGATIVE_INFINITY));
        TestCase.assertEquals(0, FloatComparisons.compare(0f, -0.0f));
        TestCase.assertEquals(0, FloatComparisons.compare(-0.0f, 0.0f));
        TestCase.assertTrue(FloatComparisons.lt(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertEquals(0, FloatComparisons.compare(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertEquals(0, FloatComparisons.compare(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.NaN, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.lt(Float.POSITIVE_INFINITY, Float.NaN));
    }
}
