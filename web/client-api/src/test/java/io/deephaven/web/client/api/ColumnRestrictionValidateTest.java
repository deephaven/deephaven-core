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
    public void testIntegerRange_nonLongWrapperIsInvalid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNotNull(r.validate(5.0));
        assertNotNull(r.validate("5"));
    }

    @Test
    public void testIntegerRange_withinBoundsIsValid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNull(r.validate(LongWrapper.of(1L)));
        assertNull(r.validate(LongWrapper.of(5L)));
        assertNull(r.validate(LongWrapper.of(10L)));
    }

    @Test
    public void testIntegerRange_belowMinIsInvalid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNotNull(r.validate(LongWrapper.of(0L)));
        assertNotNull(r.validate(LongWrapper.of(-100L)));
    }

    @Test
    public void testIntegerRange_aboveMaxIsInvalid() {
        IntegerRangeColumnRestriction r = rangedInteger(1L, 10L);
        assertNotNull(r.validate(LongWrapper.of(11L)));
    }

    @Test
    public void testIntegerRange_unboundedBelowAllowsAnyLow() {
        IntegerRangeColumnRestriction r = rangedIntegerNoMin(10L);
        assertNull(r.validate(LongWrapper.of(Long.MIN_VALUE + 1)));
        assertNull(r.validate(LongWrapper.of(10L)));
        assertNotNull(r.validate(LongWrapper.of(11L)));
    }

    @Test
    public void testIntegerRange_nullLongSentinelIsInvalid() {
        // LongWrapper.of(Long.MIN_VALUE) returns null since MIN_VALUE is the NULL_LONG sentinel
        IntegerRangeColumnRestriction r = rangedIntegerNoMin(10L);
        assertNotNull(r.validate(LongWrapper.of(Long.MIN_VALUE)));
    }

    @Test
    public void testIntegerRange_unboundedAboveAllowsAnyHigh() {
        IntegerRangeColumnRestriction r = rangedIntegerNoMax(1L);
        assertNull(r.validate(LongWrapper.of(Long.MAX_VALUE)));
        assertNull(r.validate(LongWrapper.of(1L)));
        assertNotNull(r.validate(LongWrapper.of(0L)));
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
        assertNull(r.validate(1.0));
        assertNull(r.validate(5.5));
        assertNull(r.validate(10.0));
    }

    @Test
    public void testDoubleRange_belowMinIsInvalid() {
        DoubleRangeColumnRestriction r = rangedDouble(1.0, 10.0);
        assertNotNull(r.validate(0.9));
    }

    @Test
    public void testDoubleRange_aboveMaxIsInvalid() {
        DoubleRangeColumnRestriction r = rangedDouble(1.0, 10.0);
        assertNotNull(r.validate(10.1));
    }

    @Test
    public void testDoubleRange_unboundedBelowAllowsAnyLow() {
        DoubleRangeColumnRestriction r = rangedDoubleNoMin(10.0);
        assertNull(r.validate(-1.0e300));
        assertNull(r.validate(10.0));
        assertNotNull(r.validate(10.1));
    }

    @Test
    public void testDoubleRange_unboundedAboveAllowsAnyHigh() {
        DoubleRangeColumnRestriction r = rangedDoubleNoMax(1.0);
        assertNull(r.validate(Double.MAX_VALUE));
        assertNull(r.validate(1.0));
        assertNotNull(r.validate(0.9));
    }

    // -------------------------------------------------------------------------
    // NotNullColumnRestriction
    // -------------------------------------------------------------------------

    @Test
    public void testNotNull_nullIsInvalid() {
        NotNullColumnRestriction r = new NotNullColumnRestriction();
        assertNotNull(r.validate(null));
    }

    @Test
    public void testNotNull_nonNullIsValid() {
        NotNullColumnRestriction r = new NotNullColumnRestriction();
        assertNull(r.validate("hello"));
        assertNull(r.validate(0));
        assertNull(r.validate(""));
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
        assertNotNull(r.validate(""));
    }

    @Test
    public void testNonEmpty_nonEmptyStringIsValid() {
        NonEmptyColumnRestriction r = new NonEmptyColumnRestriction();
        assertNull(r.validate("hello"));
    }

    @Test
    public void testNonEmpty_nonStringIsCoercedViaToString() {
        NonEmptyColumnRestriction r = new NonEmptyColumnRestriction();
        // Non-string types are coerced via toString() rather than throwing
        assertNull(r.validate(42));
        assertNull(r.validate(true));
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
        assertNull(r.validate("foo"));
        assertNull(r.validate("bar"));
        assertNull(r.validate("baz"));
    }

    @Test
    public void testStringList_disallowedValueIsInvalid() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of("foo", "bar"));
        assertNotNull(r.validate("qux"));
        assertNotNull(r.validate(""));
    }

    @Test
    public void testStringList_isCaseSensitive() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of("Foo"));
        assertNotNull(r.validate("foo"));
        assertNotNull(r.validate("FOO"));
        assertNull(r.validate("Foo"));
    }

    @Test
    public void testStringList_emptyListDisallowsEverything() {
        StringListColumnRestriction r = new StringListColumnRestriction(List.of());
        assertNotNull(r.validate("anything"));
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

