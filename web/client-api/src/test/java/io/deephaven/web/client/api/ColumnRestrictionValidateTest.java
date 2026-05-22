//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.proto.backplane.grpc.IntegerRangeRestriction;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ColumnRestrictionValidateTest {

    // -------------------------------------------------------------------------
    // IntegerRangeColumnRestriction
    // -------------------------------------------------------------------------

    @Test
    public void testIntegerRange_nullValueIsInvalid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNotNull(r.validate(null));
    }

    @Test
    public void testIntegerRange_withinBoundsIsValid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNull(r.validateImpl(LongWrapper.of(1L)));
        assertNull(r.validateImpl(LongWrapper.of(5L)));
        assertNull(r.validateImpl(LongWrapper.of(10L)));
    }

    @Test
    public void testIntegerRange_belowMinIsInvalid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNotNull(r.validateImpl(LongWrapper.of(0L)));
        assertNotNull(r.validateImpl(LongWrapper.of(-100L)));
    }

    @Test
    public void testIntegerRange_aboveMaxIsInvalid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNotNull(r.validateImpl(LongWrapper.of(11L)));
    }

    @Test
    public void testIntegerRange_unboundedBelowAllowsAnyLow() {
        IntegerRangeColumnRestriction r = rangedIntegerNoMin(10L);
        assertNull(r.validateImpl(LongWrapper.of(Long.MIN_VALUE + 1)));
        assertNull(r.validateImpl(LongWrapper.of(10L)));
        assertNotNull(r.validateImpl(LongWrapper.of(11L)));
    }

    @Test
    public void testIntegerRange_nullLongSentinelIsInvalid() {
        // LongWrapper.of(Long.MIN_VALUE) returns null since MIN_VALUE is the NULL_LONG sentinel
        IntegerRangeColumnRestriction r = rangedIntegerNoMin(10L);
        assertNotNull(r.validateImpl(LongWrapper.of(Long.MIN_VALUE)));
    }

    @Test
    public void testIntegerRange_unboundedAboveAllowsAnyHigh() {
        IntegerRangeColumnRestriction r = rangedIntegerNoMax(1L);
        assertNull(r.validateImpl(LongWrapper.of(Long.MAX_VALUE)));
        assertNull(r.validateImpl(LongWrapper.of(1L)));
        assertNotNull(r.validateImpl(LongWrapper.of(0L)));
    }

    // -------------------------------------------------------------------------
    // DoubleRangeColumnRestriction
    // -------------------------------------------------------------------------

    @Test
    public void testDoubleRange_nullValueIsInvalid() {
        DoubleRangeColumnRestriction r = rangedDouble(1.0, 10.0);
        assertNotNull(r.validate(null));
    }

    @Test
    public void testDoubleRange_withinBoundsIsValid() {
        DoubleRangeColumnRestriction r = rangedDouble(1.0, 10.0);
        assertNull(r.validateImpl(1.0));
        assertNull(r.validateImpl(5.5));
        assertNull(r.validateImpl(10.0));
    }

    @Test
    public void testDoubleRange_belowMinIsInvalid() {
        DoubleRangeColumnRestriction r = rangedDouble(1.0, 10.0);
        assertNotNull(r.validateImpl(0.9));
    }

    @Test
    public void testDoubleRange_aboveMaxIsInvalid() {
        DoubleRangeColumnRestriction r = rangedDouble(1.0, 10.0);
        assertNotNull(r.validateImpl(10.1));
    }

    @Test
    public void testDoubleRange_unboundedBelowAllowsAnyLow() {
        DoubleRangeColumnRestriction r = rangedDoubleNoMin(10.0);
        assertNull(r.validateImpl(-1.0e300));
        assertNull(r.validateImpl(10.0));
        assertNotNull(r.validateImpl(10.1));
    }

    @Test
    public void testDoubleRange_unboundedAboveAllowsAnyHigh() {
        DoubleRangeColumnRestriction r = rangedDoubleNoMax(1.0);
        assertNull(r.validateImpl(Double.MAX_VALUE));
        assertNull(r.validateImpl(1.0));
        assertNotNull(r.validateImpl(0.9));
    }

    // -------------------------------------------------------------------------
    // NotNullColumnRestriction
    // -------------------------------------------------------------------------

    @Test
    public void testNotNull_nullIsInvalid() {
        NotNullColumnRestriction r = new NotNullColumnRestriction();
        assertNotNull(r.validate(null));
    }

    // -------------------------------------------------------------------------
    // NonEmptyColumnRestriction
    // -------------------------------------------------------------------------

    @Test
    public void testNonEmpty_nullIsInvalid() {
        NonEmptyColumnRestriction r = new NonEmptyColumnRestriction();
        assertNotNull(r.validate(null));
    }

    @Test
    public void testNonEmpty_emptyStringIsInvalid() {
        NonEmptyColumnRestriction r = new NonEmptyColumnRestriction();
        assertNotNull(r.validateImpl(""));
    }

    @Test
    public void testNonEmpty_nonEmptyStringIsValid() {
        NonEmptyColumnRestriction r = new NonEmptyColumnRestriction();
        assertNull(r.validateImpl("hello"));
    }

    @Test
    public void testNonEmpty_nonStringIsCoercedViaToString() {
        NonEmptyColumnRestriction r = new NonEmptyColumnRestriction();
        // Non-string types are coerced via toString() rather than throwing
        assertNull(r.validateImpl("42"));
        assertNull(r.validateImpl("true"));
    }

    // -------------------------------------------------------------------------
    // StringListColumnRestriction
    // -------------------------------------------------------------------------

    @Test
    public void testStringList_nullValueIsInvalid() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of("a", "b", "c"));
        assertNotNull(r.validate(null));
    }

    @Test
    public void testStringList_allowedValueIsValid() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of("foo", "bar", "baz"));
        assertNull(r.validateImpl("foo"));
        assertNull(r.validateImpl("bar"));
        assertNull(r.validateImpl("baz"));
    }

    @Test
    public void testStringList_disallowedValueIsInvalid() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of("foo", "bar"));
        assertNotNull(r.validateImpl("qux"));
        assertNotNull(r.validateImpl(""));
    }

    @Test
    public void testStringList_isCaseSensitive() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of("Foo"));
        assertNotNull(r.validateImpl("foo"));
        assertNotNull(r.validateImpl("FOO"));
        assertNull(r.validateImpl("Foo"));
    }

    @Test
    public void testStringList_emptyListDisallowsEverything() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of());
        assertNotNull(r.validateImpl("anything"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static IntegerRangeColumnRestriction rangedInteger(long min, long max) {
        return new IntegerRangeColumnRestriction(IntegerRangeRestriction.newBuilder()
                .setMinInclusive(min)
                .setMaxInclusive(max)
                .build());
    }

    private static IntegerRangeColumnRestriction rangedIntegerNoMin(long max) {
        return new IntegerRangeColumnRestriction(IntegerRangeRestriction.newBuilder()
                .setMaxInclusive(max)
                .build());
    }

    private static IntegerRangeColumnRestriction rangedIntegerNoMax(long min) {
        return new IntegerRangeColumnRestriction(IntegerRangeRestriction.newBuilder()
                .setMinInclusive(min)
                .build());
    }

    private static DoubleRangeColumnRestriction rangedDouble(double min, double max) {
        return new DoubleRangeColumnRestriction(DoubleRangeRestriction.newBuilder()
                .setMinInclusive(min)
                .setMaxInclusive(max)
                .build());
    }

    private static DoubleRangeColumnRestriction rangedDoubleNoMin(double max) {
        return new DoubleRangeColumnRestriction(DoubleRangeRestriction.newBuilder()
                .setMaxInclusive(max)
                .build());
    }

    private static DoubleRangeColumnRestriction rangedDoubleNoMax(double min) {
        return new DoubleRangeColumnRestriction(DoubleRangeRestriction.newBuilder()
                .setMinInclusive(min)
                .build());
    }
}

