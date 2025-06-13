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

    public void testNullOrdering() {
        assertTrue(CompareUtils.compare(null, "a") < 0);
        assertTrue(CompareUtils.compare("a", null) > 0);
        assertEquals(0, CompareUtils.compare(null, null));
    }

    public void testStringOrdering() {
        assertTrue(CompareUtils.compare("a", "b") < 0);
        assertTrue(CompareUtils.compare("b", "a") > 0);
        assertEquals(0, CompareUtils.compare("a", "a"));
    }

    public void testInstantOrdering() {
        final Instant a = DateTimeUtils.parseInstant("2020-01-01T00:00:00Z");
        final Instant b = DateTimeUtils.parseInstant("2020-01-02T00:00:00Z");

        assertTrue(CompareUtils.compare(a, b) < 0);
        assertTrue(CompareUtils.compare(b, a) > 0);
        assertEquals(0, CompareUtils.compare(a, a));

        // Compare with long values
        assertTrue(CompareUtils.compare(a, DateTimeUtils.epochNanos(b)) < 0);
        assertTrue(CompareUtils.compare(DateTimeUtils.epochNanos(b), a) > 0);
        assertEquals(0, CompareUtils.compare(a, DateTimeUtils.epochNanos(a)));
        assertEquals(0, CompareUtils.compare(DateTimeUtils.epochNanos(a), a));
        assertEquals(0, CompareUtils.compare(b, DateTimeUtils.epochNanos(b)));
        assertEquals(0, CompareUtils.compare(DateTimeUtils.epochNanos(b), b));

        // Compare with long near MAX_VALUE
        final Instant maxInstant = DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE - 1_000_000_000L);
        final long maxLong = DateTimeUtils.epochNanos(maxInstant) + 1;

        assertTrue(CompareUtils.compare(maxInstant, maxLong) < 0);
        assertTrue(CompareUtils.compare(maxLong, maxInstant) > 0);
        assertEquals(0, CompareUtils.compare(maxInstant, maxInstant));
        assertEquals(0, CompareUtils.compare(maxLong, maxLong));
    }

    public void testBigInteger() {
        final BigInteger a = BigInteger.valueOf(1);
        final BigInteger b = BigInteger.valueOf(2);

        assertTrue(CompareUtils.compare(a, b) < 0);
        assertTrue(CompareUtils.compare(b, a) > 0);
        assertEquals(0, CompareUtils.compare(a, a));

        // Compare with BigDecimal values
        assertTrue(CompareUtils.compare(a, new BigDecimal(b)) < 0);
        assertTrue(CompareUtils.compare(new BigDecimal(b), a) > 0);
        assertEquals(0, CompareUtils.compare(a, new BigDecimal(a)));
        assertEquals(0, CompareUtils.compare(new BigDecimal(a), a));
        assertEquals(0, CompareUtils.compare(b, new BigDecimal(b)));
        assertEquals(0, CompareUtils.compare(new BigDecimal(b), b));
    }

    public void testBigDecimal() {
        final BigDecimal a = BigDecimal.valueOf(1);
        final BigDecimal b = BigDecimal.valueOf(2);

        assertTrue(CompareUtils.compare(a, b) < 0);
        assertTrue(CompareUtils.compare(b, a) > 0);
        assertEquals(0, CompareUtils.compare(a, a));

        // Compare with BigInteger values
        assertTrue(CompareUtils.compare(a, new BigInteger(b.toString())) < 0);
        assertTrue(CompareUtils.compare(new BigInteger(b.toString()), a) > 0);
        assertEquals(0, CompareUtils.compare(a, new BigInteger(a.toString())));
        assertEquals(0, CompareUtils.compare(new BigInteger(a.toString()), a));
        assertEquals(0, CompareUtils.compare(b, new BigInteger(b.toString())));
        assertEquals(0, CompareUtils.compare(new BigInteger(b.toString()), b));

        // Compare with char values
        assertTrue(CompareUtils.compare(a, (char) 2) < 0);
        assertTrue(CompareUtils.compare((char) 2, a) > 0);
        assertTrue(CompareUtils.compare(b, (char) 1) > 0);
        assertTrue(CompareUtils.compare((char) 1, b) < 0);
        assertEquals(0, CompareUtils.compare(a, (char) 1));
        assertEquals(0, CompareUtils.compare((char) 1, a));
        assertEquals(0, CompareUtils.compare(b, (char) 2));
        assertEquals(0, CompareUtils.compare((char) 2, b));

        // Compare with int values (stand in for all integral types)
        assertTrue(CompareUtils.compare(a, 2) < 0);
        assertTrue(CompareUtils.compare(2, a) > 0);
        assertTrue(CompareUtils.compare(b, 1) > 0);
        assertTrue(CompareUtils.compare(1, b) < 0);
        assertEquals(0, CompareUtils.compare(a, 1));
        assertEquals(0, CompareUtils.compare(1, a));
        assertEquals(0, CompareUtils.compare(b, 2));
        assertEquals(0, CompareUtils.compare(2, b));

        // Compare with double values (stand in for all float types)
        assertTrue(CompareUtils.compare(a, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, a) > 0);
        assertTrue(CompareUtils.compare(b, 1.0) > 0);
        assertTrue(CompareUtils.compare(1.0, b) < 0);
        assertEquals(0, CompareUtils.compare(a, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, a));
        assertEquals(0, CompareUtils.compare(b, 2.0));
        assertEquals(0, CompareUtils.compare(2.0, b));
    }

    public void testPrimitives() {
        // char vs byte
        assertTrue(CompareUtils.compare((char) 1, (byte) 2) < 0);
        assertTrue(CompareUtils.compare((byte) 2, (char) 1) > 0);
        assertEquals(0, CompareUtils.compare((char) 1, (byte) 1));
        assertEquals(0, CompareUtils.compare((byte) 1, (char) 1));

        // char vs short
        assertTrue(CompareUtils.compare((char) 1, (short) 2) < 0);
        assertTrue(CompareUtils.compare((short) 2, (char) 1) > 0);
        assertEquals(0, CompareUtils.compare((char) 1, (short) 1));
        assertEquals(0, CompareUtils.compare((short) 1, (char) 1));

        // char vs int
        assertTrue(CompareUtils.compare((char) 1, 2) < 0);
        assertTrue(CompareUtils.compare(2, (char) 1) > 0);
        assertEquals(0, CompareUtils.compare((char) 1, 1));
        assertEquals(0, CompareUtils.compare(1, (char) 1));

        // char vs long
        assertTrue(CompareUtils.compare((char) 1, 2L) < 0);
        assertTrue(CompareUtils.compare(2L, (char) 1) > 0);
        assertEquals(0, CompareUtils.compare((char) 1, 1L));
        assertEquals(0, CompareUtils.compare(1L, (char) 1));

        // char vs float
        assertTrue(CompareUtils.compare((char) 1, 2.0f) < 0);
        assertTrue(CompareUtils.compare(2.0f, (char) 1) > 0);
        assertEquals(0, CompareUtils.compare((char) 1, 1.0f));
        assertEquals(0, CompareUtils.compare(1.0f, (char) 1));

        // char vs double
        assertTrue(CompareUtils.compare((char) 1, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, (char) 1) > 0);
        assertEquals(0, CompareUtils.compare((char) 1, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, (char) 1));

        // byte vs short
        assertTrue(CompareUtils.compare((byte) 1, (short) 2) < 0);
        assertTrue(CompareUtils.compare((short) 2, (byte) 1) > 0);
        assertEquals(0, CompareUtils.compare((byte) 1, (short) 1));
        assertEquals(0, CompareUtils.compare((short) 1, (byte) 1));

        // byte vs int
        assertTrue(CompareUtils.compare((byte) 1, 2) < 0);
        assertTrue(CompareUtils.compare(2, (byte) 1) > 0);
        assertEquals(0, CompareUtils.compare((byte) 1, 1));
        assertEquals(0, CompareUtils.compare(1, (byte) 1));

        // byte vs long
        assertTrue(CompareUtils.compare((byte) 1, 2L) < 0);
        assertTrue(CompareUtils.compare(2L, (byte) 1) > 0);
        assertEquals(0, CompareUtils.compare((byte) 1, 1L));
        assertEquals(0, CompareUtils.compare(1L, (byte) 1));

        // byte vs float
        assertTrue(CompareUtils.compare((byte) 1, 2.0f) < 0);
        assertTrue(CompareUtils.compare(2.0f, (byte) 1) > 0);
        assertEquals(0, CompareUtils.compare((byte) 1, 1.0f));
        assertEquals(0, CompareUtils.compare(1.0f, (byte) 1));

        // byte vs double
        assertTrue(CompareUtils.compare((byte) 1, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, (byte) 1) > 0);
        assertEquals(0, CompareUtils.compare((byte) 1, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, (byte) 1));

        // short vs int
        assertTrue(CompareUtils.compare((short) 1, 2) < 0);
        assertTrue(CompareUtils.compare(2, (short) 1) > 0);
        assertEquals(0, CompareUtils.compare((short) 1, 1));
        assertEquals(0, CompareUtils.compare(1, (short) 1));

        // short vs long
        assertTrue(CompareUtils.compare((short) 1, 2L) < 0);
        assertTrue(CompareUtils.compare(2L, (short) 1) > 0);
        assertEquals(0, CompareUtils.compare((short) 1, 1L));
        assertEquals(0, CompareUtils.compare(1L, (short) 1));

        // short vs float
        assertTrue(CompareUtils.compare((short) 1, 2.0f) < 0);
        assertTrue(CompareUtils.compare(2.0f, (short) 1) > 0);
        assertEquals(0, CompareUtils.compare((short) 1, 1.0f));
        assertEquals(0, CompareUtils.compare(1.0f, (short) 1));

        // short vs double
        assertTrue(CompareUtils.compare((short) 1, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, (short) 1) > 0);
        assertEquals(0, CompareUtils.compare((short) 1, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, (short) 1));

        // int vs long
        assertTrue(CompareUtils.compare(1, 2L) < 0);
        assertTrue(CompareUtils.compare(2L, 1) > 0);
        assertEquals(0, CompareUtils.compare(1, 1L));
        assertEquals(0, CompareUtils.compare(1L, 1));

        // int vs float
        assertTrue(CompareUtils.compare(1, 2.0f) < 0);
        assertTrue(CompareUtils.compare(2.0f, 1) > 0);
        assertEquals(0, CompareUtils.compare(1, 1.0f));
        assertEquals(0, CompareUtils.compare(1.0f, 1));

        // int vs double
        assertTrue(CompareUtils.compare(1, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, 1) > 0);
        assertEquals(0, CompareUtils.compare(1, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, 1));

        // long vs float
        assertTrue(CompareUtils.compare(1L, 2.0f) < 0);
        assertTrue(CompareUtils.compare(2.0f, 1L) > 0);
        assertEquals(0, CompareUtils.compare(1L, 1.0f));
        assertEquals(0, CompareUtils.compare(1.0f, 1L));

        // long vs double
        assertTrue(CompareUtils.compare(1L, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, 1L) > 0);
        assertEquals(0, CompareUtils.compare(1L, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, 1L));

        // float vs double
        assertTrue(CompareUtils.compare(1.0f, 2.0) < 0);
        assertTrue(CompareUtils.compare(2.0, 1.0f) > 0);
        assertEquals(0, CompareUtils.compare(1.0f, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, 1.0f));
    }

    public void testBoolean() {
        // boolean vs boolean
        assertTrue(CompareUtils.compare(true, false) > 0);
        assertTrue(CompareUtils.compare(false, true) < 0);
        assertEquals(0, CompareUtils.compare(true, true));
        assertEquals(0, CompareUtils.compare(false, false));

        // boolean boxed Boolean
        assertTrue(CompareUtils.compare(true, Boolean.FALSE) > 0);
        assertTrue(CompareUtils.compare(Boolean.FALSE, true) < 0);
        assertEquals(0, CompareUtils.compare(true, Boolean.TRUE));
        assertEquals(0, CompareUtils.compare(Boolean.TRUE, true));

        // boolean vs char
        assertTrue(CompareUtils.compare(false, (char) 1) < 0);
        assertTrue(CompareUtils.compare((char) 1, false) > 0);
        assertEquals(0, CompareUtils.compare(true, (char) 1));
        assertEquals(0, CompareUtils.compare((char) 1, true));

        // boolean vs byte
        assertTrue(CompareUtils.compare(false, (byte) 1) < 0);
        assertTrue(CompareUtils.compare((byte) 1, false) > 0);
        assertEquals(0, CompareUtils.compare(true, (byte) 1));
        assertEquals(0, CompareUtils.compare((byte) 1, true));

        // boolean vs short
        assertTrue(CompareUtils.compare(false, (short) 1) < 0);
        assertTrue(CompareUtils.compare((short) 1, false) > 0);
        assertEquals(0, CompareUtils.compare(true, (short) 1));
        assertEquals(0, CompareUtils.compare((short) 1, true));

        // boolean vs int
        assertTrue(CompareUtils.compare(false, 1) < 0);
        assertTrue(CompareUtils.compare(1, false) > 0);
        assertEquals(0, CompareUtils.compare(true, 1));
        assertEquals(0, CompareUtils.compare(1, true));

        // boolean vs long
        assertTrue(CompareUtils.compare(false, 1L) < 0);
        assertTrue(CompareUtils.compare(1L, false) > 0);
        assertEquals(0, CompareUtils.compare(true, 1L));
        assertEquals(0, CompareUtils.compare(1L, true));

        // boolean vs float
        assertTrue(CompareUtils.compare(false, 1.0f) < 0);
        assertTrue(CompareUtils.compare(1.0f, false) > 0);
        assertEquals(0, CompareUtils.compare(true, 1.0f));
        assertEquals(0, CompareUtils.compare(1.0f, true));

        // boolean vs double
        assertTrue(CompareUtils.compare(false, 1.0) < 0);
        assertTrue(CompareUtils.compare(1.0, false) > 0);
        assertEquals(0, CompareUtils.compare(true, 1.0));
        assertEquals(0, CompareUtils.compare(1.0, true));

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
