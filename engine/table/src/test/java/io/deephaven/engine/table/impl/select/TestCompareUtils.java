//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.time.DateTimeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

public class TestCompareUtils extends RefreshingTableTestCase {

    /** Assert that {@code lower < upper}. The method also verifies the flipped comparison. */
    private static void assertLt(final Object lower, final Object upper) {
        assertTrue(CompareUtils.compare(lower, upper) < 0);
        assertTrue(CompareUtils.compare(upper, lower) > 0);
    }

    /** Assert that {@code a == b}. The method also verifies the flipped comparison. */
    private static void assertEq(final Object a, final Object b) {
        assertEquals(0, CompareUtils.compare(a, b));
        assertEquals(0, CompareUtils.compare(b, a));
    }

    public void testNullOrdering() {
        assertLt(null, "a");
        assertEq(null, null);
    }

    public void testStringOrdering() {
        assertLt("a", "b");
        assertEq("a", "a");
    }

    public void testInstantOrdering() {
        final Instant a = DateTimeUtils.parseInstant("2020-01-01T00:00:00Z");
        final Instant b = DateTimeUtils.parseInstant("2020-01-02T00:00:00Z");

        assertLt(a, b);
        assertEq(a, a);

        // Compare with long values
        assertLt(a, DateTimeUtils.epochNanos(b));
        assertEq(a, DateTimeUtils.epochNanos(a));
        assertEq(b, DateTimeUtils.epochNanos(b));

        // Compare with long near MAX_VALUE
        final Instant maxInstant = DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE - 1_000_000_000L);
        final long maxLong = DateTimeUtils.epochNanos(maxInstant) + 1;

        assertLt(maxInstant, maxLong);
        assertEq(maxInstant, maxInstant);
        assertEq(maxLong, maxLong);
    }

    public void testBigInteger() {
        final BigInteger a = BigInteger.valueOf(1);
        final BigInteger b = BigInteger.valueOf(2);

        assertLt(a, b);
        assertEq(a, a);

        // Compare with BigDecimal values
        assertLt(a, new BigDecimal(b));
        assertEq(a, new BigDecimal(a));
        assertEq(b, new BigDecimal(b));
    }

    public void testBigDecimal() {
        final BigDecimal a = BigDecimal.valueOf(1);
        final BigDecimal b = BigDecimal.valueOf(2);

        assertLt(a, b);
        assertEq(a, a);

        // Compare with BigInteger values
        assertLt(a, new BigInteger(b.toString()));
        assertEq(a, new BigInteger(a.toString()));
        assertEq(b, new BigInteger(b.toString()));

        // Compare with char values
        assertLt(a, (char) 2);
        assertEq(a, (char) 1);
        assertEq(b, (char) 2);

        // Compare with int values (stand‑in for all integral types)
        assertLt(a, 2);
        assertEq(a, 1);
        assertEq(b, 2);

        // Compare with double values (stand‑in for all float types)
        assertLt(a, 2.0);
        assertEq(a, 1.0);
        assertEq(b, 2.0);
    }

    public void testPrimitives() {
        // char vs byte
        assertLt((char) 1, (byte) 2);
        assertEq((char) 1, (byte) 1);

        // char vs short
        assertLt((char) 1, (short) 2);
        assertEq((char) 1, (short) 1);

        // char vs int
        assertLt((char) 1, 2);
        assertEq((char) 1, 1);

        // char vs long
        assertLt((char) 1, 2L);
        assertEq((char) 1, 1L);

        // char vs float
        assertLt((char) 1, 2.0f);
        assertEq((char) 1, 1.0f);

        // char vs double
        assertLt((char) 1, 2.0);
        assertEq((char) 1, 1.0);

        // byte vs short
        assertLt((byte) 1, (short) 2);
        assertEq((byte) 1, (short) 1);

        // byte vs int
        assertLt((byte) 1, 2);
        assertEq((byte) 1, 1);

        // byte vs long
        assertLt((byte) 1, 2L);
        assertEq((byte) 1, 1L);

        // byte vs float
        assertLt((byte) 1, 2.0f);
        assertEq((byte) 1, 1.0f);

        // byte vs double
        assertLt((byte) 1, 2.0);
        assertEq((byte) 1, 1.0);

        // short vs int
        assertLt((short) 1, 2);
        assertEq((short) 1, 1);

        // short vs long
        assertLt((short) 1, 2L);
        assertEq((short) 1, 1L);

        // short vs float
        assertLt((short) 1, 2.0f);
        assertEq((short) 1, 1.0f);

        // short vs double
        assertLt((short) 1, 2.0);
        assertEq((short) 1, 1.0);

        // int vs long
        assertLt(1, 2L);
        assertEq(1, 1L);

        // int vs float
        assertLt(1, 2.0f);
        assertEq(1, 1.0f);

        // int vs double
        assertLt(1, 2.0);
        assertEq(1, 1.0);

        // long vs float
        assertLt(1L, 2.0f);
        assertEq(1L, 1.0f);

        // long vs double
        assertLt(1L, 2.0);
        assertEq(1L, 1.0);

        // float vs double
        assertLt(1.0f, 2.0);
        assertEq(1.0f, 1.0);
    }

    public void testBoolean() {
        // boolean vs boolean
        assertLt(false, true);
        assertEq(true, true);
        assertEq(false, false);

        // boolean vs boxed Boolean
        assertLt(Boolean.FALSE, true);
        assertEq(true, Boolean.TRUE);

        // boolean vs char
        assertLt(false, (char) 1);
        assertEq(true, (char) 1);

        // boolean vs byte
        assertLt(false, (byte) 1);
        assertEq(true, (byte) 1);

        // boolean vs short
        assertLt(false, (short) 1);
        assertEq(true, (short) 1);

        // boolean vs int
        assertLt(false, 1);
        assertEq(true, 1);

        // boolean vs long
        assertLt(false, 1L);
        assertEq(true, 1L);

        // boolean vs float
        assertLt(false, 1.0f);
        assertEq(true, 1.0f);

        // boolean vs double
        assertLt(false, 1.0);
        assertEq(true, 1.0);
    }

    public void testErrorHandling() {
        try {
            CompareUtils.compare(new Object(), "A");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }
}
