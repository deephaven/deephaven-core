//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.tuple.serialization;

import io.deephaven.tuple.ArrayTuple;
import io.deephaven.tuple.generated.*;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.*;

public class TestTupleComparison {

    @Test
    public void testTupleWithByteComparison() {
        // Test IntByte tuples
        final IntByteTuple nullTuple = new IntByteTuple(0, NULL_BYTE);
        final IntByteTuple normalTuple = new IntByteTuple(0, (byte) 1);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntByteTuple(0, NULL_BYTE)));
        assertEquals(0, nullTuple.compareTo(new IntByteTuple(0, NULL_BYTE)));

        assertTrue(normalTuple.equals(new IntByteTuple(0, (byte) 1)));
        assertEquals(0, normalTuple.compareTo(new IntByteTuple(0, (byte) 1)));
    }

    @Test
    public void testTupleWithCharComparison() throws Exception {
        // Test IntChar tuples
        final IntCharTuple nullTuple = new IntCharTuple(0, NULL_CHAR);
        final IntCharTuple normalTuple = new IntCharTuple(0, 'A');

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntCharTuple(0, NULL_CHAR)));
        assertEquals(0, nullTuple.compareTo(new IntCharTuple(0, NULL_CHAR)));

        assertTrue(normalTuple.equals(new IntCharTuple(0, 'A')));
        assertEquals(0, normalTuple.compareTo(new IntCharTuple(0, 'A')));
    }

    @Test
    public void testTupleWithShortComparison() {
        // Test IntShort tuples
        final IntShortTuple nullTuple = new IntShortTuple(0, NULL_SHORT);
        final IntShortTuple normalTuple = new IntShortTuple(0, (short) 1);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntShortTuple(0, NULL_SHORT)));
        assertEquals(0, nullTuple.compareTo(new IntShortTuple(0, NULL_SHORT)));

        assertTrue(normalTuple.equals(new IntShortTuple(0, (short) 1)));
        assertEquals(0, normalTuple.compareTo(new IntShortTuple(0, (short) 1)));
    }

    @Test
    public void testTupleWithIntComparison() {
        // Test IntInt tuples
        final IntIntTuple nullTuple = new IntIntTuple(0, NULL_INT);
        final IntIntTuple normalTuple = new IntIntTuple(0, 1);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntIntTuple(0, NULL_INT)));
        assertEquals(0, nullTuple.compareTo(new IntIntTuple(0, NULL_INT)));

        assertTrue(normalTuple.equals(new IntIntTuple(0, 1)));
        assertEquals(0, normalTuple.compareTo(new IntIntTuple(0, 1)));
    }

    @Test
    public void testTupleWithLongComparison() {
        // Test IntLong tuples
        final IntLongTuple nullTuple = new IntLongTuple(0, NULL_LONG);
        final IntLongTuple normalTuple = new IntLongTuple(0, 1);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntLongTuple(0, NULL_LONG)));
        assertEquals(0, nullTuple.compareTo(new IntLongTuple(0, NULL_LONG)));

        assertTrue(normalTuple.equals(new IntLongTuple(0, 1)));
        assertEquals(0, normalTuple.compareTo(new IntLongTuple(0, 1)));
    }

    @Test
    public void testTupleWithFloatComparison() {
        // Test IntFloat tuples
        final IntFloatTuple nullTuple = new IntFloatTuple(0, NULL_FLOAT);
        final IntFloatTuple negInfTuple = new IntFloatTuple(0, Float.NEGATIVE_INFINITY);
        final IntFloatTuple negZeroTuple = new IntFloatTuple(0, -0.0F);
        final IntFloatTuple posZeroTuple = new IntFloatTuple(0, 0.0F);
        final IntFloatTuple normalTuple = new IntFloatTuple(0, 1.0F);
        final IntFloatTuple posInfTuple = new IntFloatTuple(0, Float.POSITIVE_INFINITY);
        final IntFloatTuple nanTuple = new IntFloatTuple(0, Float.NaN);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(negInfTuple) < 0);
        assertTrue(negInfTuple.compareTo(negZeroTuple) < 0);
        assertTrue(negZeroTuple.compareTo(posZeroTuple) == 0); // special case
        assertTrue(posZeroTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(posInfTuple) < 0);
        assertTrue(posInfTuple.compareTo(nanTuple) < 0);

        assertTrue(nanTuple.compareTo(posInfTuple) > 0);
        assertTrue(posInfTuple.compareTo(normalTuple) > 0);
        assertTrue(normalTuple.compareTo(posZeroTuple) > 0);
        assertTrue(posZeroTuple.compareTo(negZeroTuple) == 0); // special case
        assertTrue(negZeroTuple.compareTo(negInfTuple) > 0);
        assertTrue(negInfTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(negInfTuple.equals(negInfTuple));
        assertEquals(0, negInfTuple.compareTo(negInfTuple));

        assertTrue(negZeroTuple.equals(negZeroTuple));
        assertEquals(0, negZeroTuple.compareTo(negZeroTuple));

        assertTrue(posZeroTuple.equals(posZeroTuple));
        assertEquals(0, posZeroTuple.compareTo(posZeroTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        assertTrue(posInfTuple.equals(posInfTuple));
        assertEquals(0, posInfTuple.compareTo(posInfTuple));

        assertTrue(nanTuple.equals(nanTuple));
        assertEquals(0, nanTuple.compareTo(nanTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntFloatTuple(0, NULL_FLOAT)));
        assertEquals(0, nullTuple.compareTo(new IntFloatTuple(0, NULL_FLOAT)));

        assertTrue(negInfTuple.equals(new IntFloatTuple(0, Float.NEGATIVE_INFINITY)));
        assertEquals(0, negInfTuple.compareTo(new IntFloatTuple(0, Float.NEGATIVE_INFINITY)));

        assertTrue(negZeroTuple.equals(new IntFloatTuple(0, -0.0F)));
        assertEquals(0, negZeroTuple.compareTo(new IntFloatTuple(0, -0.0F)));
        assertTrue(negZeroTuple.equals(new IntFloatTuple(0, 0.0F))); // special case
        assertEquals(0, negZeroTuple.compareTo(new IntFloatTuple(0, 0.0F))); // special case

        assertTrue(posZeroTuple.equals(new IntFloatTuple(0, 0.0F)));
        assertEquals(0, posZeroTuple.compareTo(new IntFloatTuple(0, 0.0F)));
        assertTrue(posZeroTuple.equals(new IntFloatTuple(0, -0.0F))); // special case
        assertEquals(0, posZeroTuple.compareTo(new IntFloatTuple(0, -0.0F))); // special case

        assertTrue(normalTuple.equals(new IntFloatTuple(0, 1.0F)));
        assertEquals(0, normalTuple.compareTo(new IntFloatTuple(0, 1.0F)));

        assertTrue(posInfTuple.equals(new IntFloatTuple(0, Float.POSITIVE_INFINITY)));
        assertEquals(0, posInfTuple.compareTo(new IntFloatTuple(0, Float.POSITIVE_INFINITY)));

        assertTrue(nanTuple.equals(new IntFloatTuple(0, Float.NaN)));
        assertEquals(0, nanTuple.compareTo(new IntFloatTuple(0, Float.NaN)));
    }

    @Test
    public void testTupleWithDoubleComparison() {
        // Test IntDouble tuples
        final IntDoubleTuple nullTuple = new IntDoubleTuple(0, NULL_DOUBLE);
        final IntDoubleTuple negInfTuple = new IntDoubleTuple(0, Double.NEGATIVE_INFINITY);
        final IntDoubleTuple negZeroTuple = new IntDoubleTuple(0, -0.0);
        final IntDoubleTuple posZeroTuple = new IntDoubleTuple(0, 0.0);
        final IntDoubleTuple normalTuple = new IntDoubleTuple(0, 1.0);
        final IntDoubleTuple posInfTuple = new IntDoubleTuple(0, Double.POSITIVE_INFINITY);
        final IntDoubleTuple nanTuple = new IntDoubleTuple(0, Double.NaN);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(negInfTuple) < 0);
        assertTrue(negInfTuple.compareTo(negZeroTuple) < 0);
        assertTrue(negZeroTuple.compareTo(posZeroTuple) == 0); // special case
        assertTrue(posZeroTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(posInfTuple) < 0);
        assertTrue(posInfTuple.compareTo(nanTuple) < 0);

        assertTrue(nanTuple.compareTo(posInfTuple) > 0);
        assertTrue(posInfTuple.compareTo(normalTuple) > 0);
        assertTrue(normalTuple.compareTo(posZeroTuple) > 0);
        assertTrue(posZeroTuple.compareTo(negZeroTuple) == 0); // special case
        assertTrue(negZeroTuple.compareTo(negInfTuple) > 0);
        assertTrue(negInfTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(negInfTuple.equals(negInfTuple));
        assertEquals(0, negInfTuple.compareTo(negInfTuple));

        assertTrue(negZeroTuple.equals(negZeroTuple));
        assertEquals(0, negZeroTuple.compareTo(negZeroTuple));

        assertTrue(posZeroTuple.equals(posZeroTuple));
        assertEquals(0, posZeroTuple.compareTo(posZeroTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        assertTrue(posInfTuple.equals(posInfTuple));
        assertEquals(0, posInfTuple.compareTo(posInfTuple));

        assertTrue(nanTuple.equals(nanTuple));
        assertEquals(0, nanTuple.compareTo(nanTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntDoubleTuple(0, NULL_DOUBLE)));
        assertEquals(0, nullTuple.compareTo(new IntDoubleTuple(0, NULL_DOUBLE)));

        assertTrue(negInfTuple.equals(new IntDoubleTuple(0, Double.NEGATIVE_INFINITY)));
        assertEquals(0, negInfTuple.compareTo(new IntDoubleTuple(0, Double.NEGATIVE_INFINITY)));

        assertTrue(negZeroTuple.equals(new IntDoubleTuple(0, -0.0)));
        assertEquals(0, negZeroTuple.compareTo(new IntDoubleTuple(0, -0.0)));
        assertTrue(negZeroTuple.equals(new IntDoubleTuple(0, 0.0))); // special case
        assertEquals(0, negZeroTuple.compareTo(new IntDoubleTuple(0, 0.0))); // special case

        assertTrue(posZeroTuple.equals(new IntDoubleTuple(0, 0.0)));
        assertEquals(0, posZeroTuple.compareTo(new IntDoubleTuple(0, 0.0)));
        assertTrue(posZeroTuple.equals(new IntDoubleTuple(0, -0.0))); // special case
        assertEquals(0, posZeroTuple.compareTo(new IntDoubleTuple(0, -0.0))); // special case

        assertTrue(normalTuple.equals(new IntDoubleTuple(0, 1.0)));
        assertEquals(0, normalTuple.compareTo(new IntDoubleTuple(0, 1.0)));

        assertTrue(posInfTuple.equals(new IntDoubleTuple(0, Double.POSITIVE_INFINITY)));
        assertEquals(0, posInfTuple.compareTo(new IntDoubleTuple(0, Double.POSITIVE_INFINITY)));

        assertTrue(nanTuple.equals(new IntDoubleTuple(0, Double.NaN)));
        assertEquals(0, nanTuple.compareTo(new IntDoubleTuple(0, Double.NaN)));
    }

    @Test
    public void testTupleWithObjectComparison() {
        // Test IntObject tuples
        final IntObjectTuple nullTuple = new IntObjectTuple(0, null);
        final IntObjectTuple normalTuple = new IntObjectTuple(0, "A");

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new IntObjectTuple(0, null)));
        assertEquals(0, nullTuple.compareTo(new IntObjectTuple(0, null)));

        assertTrue(normalTuple.equals(new IntObjectTuple(0, "A")));
        assertEquals(0, normalTuple.compareTo(new IntObjectTuple(0, "A")));
    }

    @Test
    public void testArrayTupleWithCharComparison() throws Exception {
        // Test ArrayTuple tuples
        final ArrayTuple nullTuple = new ArrayTuple(0, null);
        final ArrayTuple normalTuple = new ArrayTuple(0, 'A');

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new ArrayTuple(0, null)));
        assertEquals(0, nullTuple.compareTo(new ArrayTuple(0, null)));

        assertTrue(normalTuple.equals(new ArrayTuple(0, 'A')));
        assertEquals(0, normalTuple.compareTo(new ArrayTuple(0, 'A')));
    }

    @Test
    public void testArrayTupleWithFloatComparison() {
        // Test ArrayTuple tuples
        final ArrayTuple nullTuple = new ArrayTuple(0, null);
        final ArrayTuple negInfTuple = new ArrayTuple(0, Float.NEGATIVE_INFINITY);
        final ArrayTuple zeroTuple = new ArrayTuple(0, 0.0f);
        final ArrayTuple normalTuple = new ArrayTuple(0, 1.0f);
        final ArrayTuple posInfTuple = new ArrayTuple(0, Float.POSITIVE_INFINITY);
        final ArrayTuple nanTuple = new ArrayTuple(0, Float.NaN);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(negInfTuple) < 0);
        assertTrue(negInfTuple.compareTo(zeroTuple) < 0);
        assertTrue(zeroTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(posInfTuple) < 0);
        assertTrue(posInfTuple.compareTo(nanTuple) < 0);

        assertTrue(nanTuple.compareTo(posInfTuple) > 0);
        assertTrue(posInfTuple.compareTo(normalTuple) > 0);
        assertTrue(normalTuple.compareTo(zeroTuple) > 0);
        assertTrue(zeroTuple.compareTo(negInfTuple) > 0);
        assertTrue(negInfTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(negInfTuple.equals(negInfTuple));
        assertEquals(0, negInfTuple.compareTo(negInfTuple));

        assertTrue(zeroTuple.equals(zeroTuple));
        assertEquals(0, zeroTuple.compareTo(zeroTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        assertTrue(posInfTuple.equals(posInfTuple));
        assertEquals(0, posInfTuple.compareTo(posInfTuple));

        assertTrue(nanTuple.equals(nanTuple));
        assertEquals(0, nanTuple.compareTo(nanTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new ArrayTuple(0, null)));
        assertEquals(0, nullTuple.compareTo(new ArrayTuple(0, null)));

        assertTrue(negInfTuple.equals(new ArrayTuple(0, Float.NEGATIVE_INFINITY)));
        assertEquals(0, negInfTuple.compareTo(new ArrayTuple(0, Float.NEGATIVE_INFINITY)));

        assertTrue(zeroTuple.equals(new ArrayTuple(0, 0.0f))); // special case
        assertEquals(0, zeroTuple.compareTo(new ArrayTuple(0, 0.0f))); // special case

        assertTrue(normalTuple.equals(new ArrayTuple(0, 1.0f)));
        assertEquals(0, normalTuple.compareTo(new ArrayTuple(0, 1.0f)));

        assertTrue(posInfTuple.equals(new ArrayTuple(0, Float.POSITIVE_INFINITY)));
        assertEquals(0, posInfTuple.compareTo(new ArrayTuple(0, Float.POSITIVE_INFINITY)));

        assertTrue(nanTuple.equals(new ArrayTuple(0, Float.NaN)));
        assertEquals(0, nanTuple.compareTo(new ArrayTuple(0, Float.NaN)));
    }

    @Test
    public void testArrayTupleWithDoubleComparison() {
        // Test ArrayTuple tuples
        final ArrayTuple nullTuple = new ArrayTuple(0, null);
        final ArrayTuple negInfTuple = new ArrayTuple(0, Double.NEGATIVE_INFINITY);
        final ArrayTuple zeroTuple = new ArrayTuple(0, 0.0);
        final ArrayTuple normalTuple = new ArrayTuple(0, 1.0);
        final ArrayTuple posInfTuple = new ArrayTuple(0, Double.POSITIVE_INFINITY);
        final ArrayTuple nanTuple = new ArrayTuple(0, Double.NaN);

        // Less than / greater than tests
        assertTrue(nullTuple.compareTo(negInfTuple) < 0);
        assertTrue(negInfTuple.compareTo(zeroTuple) < 0);
        assertTrue(zeroTuple.compareTo(normalTuple) < 0);
        assertTrue(normalTuple.compareTo(posInfTuple) < 0);
        assertTrue(posInfTuple.compareTo(nanTuple) < 0);

        assertTrue(nanTuple.compareTo(posInfTuple) > 0);
        assertTrue(posInfTuple.compareTo(normalTuple) > 0);
        assertTrue(normalTuple.compareTo(zeroTuple) > 0);
        assertTrue(zeroTuple.compareTo(negInfTuple) > 0);
        assertTrue(negInfTuple.compareTo(nullTuple) > 0);

        // Reference equality tests
        assertTrue(nullTuple.equals(nullTuple));
        assertEquals(0, nullTuple.compareTo(nullTuple));

        assertTrue(negInfTuple.equals(negInfTuple));
        assertEquals(0, negInfTuple.compareTo(negInfTuple));

        assertTrue(zeroTuple.equals(zeroTuple));
        assertEquals(0, zeroTuple.compareTo(zeroTuple));

        assertTrue(normalTuple.equals(normalTuple));
        assertEquals(0, normalTuple.compareTo(normalTuple));

        assertTrue(posInfTuple.equals(posInfTuple));
        assertEquals(0, posInfTuple.compareTo(posInfTuple));

        assertTrue(nanTuple.equals(nanTuple));
        assertEquals(0, nanTuple.compareTo(nanTuple));

        // Equivalence tests
        assertTrue(nullTuple.equals(new ArrayTuple(0, null)));
        assertEquals(0, nullTuple.compareTo(new ArrayTuple(0, null)));

        assertTrue(negInfTuple.equals(new ArrayTuple(0, Double.NEGATIVE_INFINITY)));
        assertEquals(0, negInfTuple.compareTo(new ArrayTuple(0, Double.NEGATIVE_INFINITY)));

        assertTrue(zeroTuple.equals(new ArrayTuple(0, 0.0)));
        assertEquals(0, zeroTuple.compareTo(new ArrayTuple(0, 0.0)));

        assertTrue(normalTuple.equals(new ArrayTuple(0, 1.0)));
        assertEquals(0, normalTuple.compareTo(new ArrayTuple(0, 1.0)));

        assertTrue(posInfTuple.equals(new ArrayTuple(0, Double.POSITIVE_INFINITY)));
        assertEquals(0, posInfTuple.compareTo(new ArrayTuple(0, Double.POSITIVE_INFINITY)));

        assertTrue(nanTuple.equals(new ArrayTuple(0, Double.NaN)));
        assertEquals(0, nanTuple.compareTo(new ArrayTuple(0, Double.NaN)));
    }

    /**
     * Should be enabled when https://deephaven.atlassian.net/browse/DH-20943 is resolved.
     */
    @Ignore
    @Test
    public void testArrayTupleWithFloatNegPosZeroEquality() {
        // Test ArrayTuple tuples
        final ArrayTuple negZeroTuple = new ArrayTuple(0, -0.0f);
        final ArrayTuple posZeroTuple = new ArrayTuple(0, 0.0f);

        assertTrue(negZeroTuple.compareTo(posZeroTuple) == 0); // special case
        assertTrue(posZeroTuple.compareTo(negZeroTuple) == 0); // special case

        assertTrue(negZeroTuple.equals(negZeroTuple));
        assertEquals(0, negZeroTuple.compareTo(negZeroTuple));

        assertTrue(posZeroTuple.equals(posZeroTuple));
        assertEquals(0, posZeroTuple.compareTo(posZeroTuple));

        // Equivalence tests
        assertTrue(negZeroTuple.equals(new ArrayTuple(0, -0.0f)));
        assertEquals(0, negZeroTuple.compareTo(new ArrayTuple(0, -0.0f)));
        assertTrue(negZeroTuple.equals(new ArrayTuple(0, 0.0f))); // special case
        assertEquals(0, negZeroTuple.compareTo(new ArrayTuple(0, 0.0f))); // special case

        assertTrue(posZeroTuple.equals(new ArrayTuple(0, 0.0f)));
        assertEquals(0, posZeroTuple.compareTo(new ArrayTuple(0, 0.0f)));
        assertTrue(posZeroTuple.equals(new ArrayTuple(0, -0.0f))); // special case
        assertEquals(0, posZeroTuple.compareTo(new ArrayTuple(0, -0.0f))); // special case
    }

    /**
     * Should be enabled when https://deephaven.atlassian.net/browse/DH-20943 is resolved.
     */
    @Ignore
    @Test
    public void testArrayTupleWithDoubleNegPosZeroEquality() {
        // Test ArrayTuple tuples
        final ArrayTuple negZeroTuple = new ArrayTuple(0, -0.0);
        final ArrayTuple posZeroTuple = new ArrayTuple(0, 0.0);

        assertTrue(negZeroTuple.compareTo(posZeroTuple) == 0); // special case
        assertTrue(posZeroTuple.compareTo(negZeroTuple) == 0); // special case

        assertTrue(negZeroTuple.equals(negZeroTuple));
        assertEquals(0, negZeroTuple.compareTo(negZeroTuple));

        assertTrue(posZeroTuple.equals(posZeroTuple));
        assertEquals(0, posZeroTuple.compareTo(posZeroTuple));

        // Equivalence tests
        assertTrue(negZeroTuple.equals(new ArrayTuple(0, -0.0)));
        assertEquals(0, negZeroTuple.compareTo(new ArrayTuple(0, -0.0)));
        assertTrue(negZeroTuple.equals(new ArrayTuple(0, 0.0))); // special case
        assertEquals(0, negZeroTuple.compareTo(new ArrayTuple(0, 0.0))); // special case

        assertTrue(posZeroTuple.equals(new ArrayTuple(0, 0.0)));
        assertEquals(0, posZeroTuple.compareTo(new ArrayTuple(0, 0.0)));
        assertTrue(posZeroTuple.equals(new ArrayTuple(0, -0.0))); // special case
        assertEquals(0, posZeroTuple.compareTo(new ArrayTuple(0, -0.0))); // special case
    }

    private static Object[] randomizedCopy(final Object[] array, final long seed) {
        final Object[] result = array.clone();
        final Random random = new Random(seed);

        // Iterate from the last element down to the second element
        for (int i = result.length - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);

            // Swap the element at index 'i' with the element at index 'j'
            Object temp = result[i];
            result[i] = result[j];
            result[j] = temp;
        }
        return result;
    }

    @Test
    public void testLexicographicalOrderSize2() {
        final Object[] expected = new Object[] {
                new IntByteTuple(0, (byte) 0),
                new IntByteTuple(0, (byte) 1),
                new IntByteTuple(0, (byte) 2),
                new IntByteTuple(1, (byte) 0),
                new IntByteTuple(1, (byte) 1),
                new IntByteTuple(1, (byte) 2),
                new IntByteTuple(2, (byte) 0),
                new IntByteTuple(2, (byte) 1),
                new IntByteTuple(2, (byte) 2)
        };

        // Test a few random orderings to ensure that lexicographical order is restored.
        for (final long seed : new long[] {0L, 0xDEADBEEFL, 0xBADDCAFEL, 0xFEEDFACEL, 0xCAFEBABEL}) {
            final Object[] toSort = randomizedCopy(expected, seed);
            Arrays.sort(toSort);
            assertEquals("Failed with seed " + seed,
                    Arrays.asList(expected),
                    Arrays.asList(toSort));
        }
    }

    @Test
    public void testLexicographicalOrderSize3() {
        final Object[] expected = new Object[] {
                new IntByteShortTuple(0, (byte) 0, (short) 0),
                new IntByteShortTuple(0, (byte) 0, (short) 1),
                new IntByteShortTuple(0, (byte) 1, (short) 0),
                new IntByteShortTuple(0, (byte) 1, (short) 1),
                new IntByteShortTuple(1, (byte) 0, (short) 0),
                new IntByteShortTuple(1, (byte) 0, (short) 1),
                new IntByteShortTuple(1, (byte) 1, (short) 0),
                new IntByteShortTuple(1, (byte) 1, (short) 1)
        };

        // Test a few random orderings to ensure that lexicographical order is restored.
        for (final long seed : new long[] {0L, 0xDEADBEEFL, 0xBADDCAFEL, 0xFEEDFACEL, 0xCAFEBABEL}) {
            final Object[] toSort = randomizedCopy(expected, seed);
            Arrays.sort(toSort);
            assertEquals("Failed with seed " + seed,
                    Arrays.asList(expected),
                    Arrays.asList(toSort));
        }
    }
}
