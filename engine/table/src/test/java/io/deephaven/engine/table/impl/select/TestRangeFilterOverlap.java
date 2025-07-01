//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.*;

public class TestRangeFilterOverlap extends RefreshingTableTestCase {

    public void testCharRangeFilterOverlap() {
        CharRangeFilter f;

        // No overlap, f < range
        f = new CharRangeFilter("A", 'a', 'b', true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps('c', 'd'));

        // No overlap, f > range
        f = new CharRangeFilter("A", 'c', 'd', true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps('a', 'b'));

        // No overlap, f ends at range
        f = new CharRangeFilter("A", 'a', 'b', true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps('b', 'c'));

        // Overlap, f ends at range but both inclusive
        f = new CharRangeFilter("A", 'a', 'b', true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps('b', 'c'));

        // Overlap, range ends at f but both inclusive
        f = new CharRangeFilter("A", 'b', 'c', true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps('a', 'b'));

        // Overlap, f entirely inside range
        f = new CharRangeFilter("A", 'b', 'c', true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps('a', 'd'));

        // Overlap, range entirely inside f
        f = new CharRangeFilter("A", 'a', 'd', true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps('b', 'c'));

        // Not containing, val < f
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        f.initChunkFilter();
        assertFalse(f.matches('a'));

        // Not containing, val > f
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        f.initChunkFilter();
        assertFalse(f.matches('e'));

        // Containing, middle
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        f.initChunkFilter();
        assertTrue(f.matches('c'));

        // Containing, start
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        f.initChunkFilter();
        assertTrue(f.matches('b'));

        // Containing, end
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        f.initChunkFilter();
        assertTrue(f.matches('d'));

        // Containing, middle
        f = new CharRangeFilter("A", 'b', 'd', false, false);
        f.initChunkFilter();
        assertTrue(f.matches('c'));

        // Not containing, start
        f = new CharRangeFilter("A", 'b', 'd', false, false);
        f.initChunkFilter();
        assertFalse(f.matches('b'));

        // Not containing, end
        f = new CharRangeFilter("A", 'b', 'd', false, false);
        f.initChunkFilter();
        assertFalse(f.matches('d'));

        // Containing null
        f = new CharRangeFilter("A", NULL_CHAR, 'd', true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_CHAR));
        assertTrue(f.overlaps(NULL_CHAR, NULL_CHAR));

        // Not containing null
        f = new CharRangeFilter("A", NULL_CHAR, 'd', false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_CHAR));
        assertFalse(f.overlaps(NULL_CHAR, NULL_CHAR));

        // Not containing null, normal
        f = new CharRangeFilter("A", 'b', 'd', true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_CHAR));

        f = new CharRangeFilter("A", NULL_CHAR, NULL_CHAR, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_CHAR));
        assertTrue(f.overlaps(NULL_CHAR, NULL_CHAR));

        f = new CharRangeFilter("A", 'b', 'd', true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_CHAR, 'a')); // before filter
        assertTrue(f.overlaps(NULL_CHAR, 'b')); // touches at lower bound
    }

    public void testByteRangeFilterOverlap() {
        ByteRangeFilter f;

        // No overlap, f < range
        f = new ByteRangeFilter("A", (byte) 1, (byte) 2, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps((byte) 3, (byte) 4));

        // No overlap, f > range
        f = new ByteRangeFilter("A", (byte) 3, (byte) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps((byte) 1, (byte) 2));

        // No overlap, f ends at range
        f = new ByteRangeFilter("A", (byte) 1, (byte) 2, true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps((byte) 2, (byte) 3));

        // Overlap, f ends at range but both inclusive
        f = new ByteRangeFilter("A", (byte) 1, (byte) 2, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((byte) 2, (byte) 3));

        // Overlap, range ends at f but both inclusive
        f = new ByteRangeFilter("A", (byte) 2, (byte) 3, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((byte) 1, (byte) 2));

        // Overlap, f entirely inside range
        f = new ByteRangeFilter("A", (byte) 2, (byte) 3, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((byte) 1, (byte) 4));

        // Overlap, range entirely inside f
        f = new ByteRangeFilter("A", (byte) 1, (byte) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((byte) 2, (byte) 3));

        // Not containing, val < f
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.matches((byte) 1));

        // Not containing, val > f
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.matches((byte) 5));

        // Containing, middle
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches((byte) 3));

        // Containing, start
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches((byte) 2));

        // Containing, end
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches((byte) 4));

        // Containing, middle
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, false, false);
        f.initChunkFilter();
        assertTrue(f.matches((byte) 3));

        // Not containing, start
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, false, false);
        f.initChunkFilter();
        assertFalse(f.matches((byte) 2));

        // Not containing, end
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, false, false);
        f.initChunkFilter();
        assertFalse(f.matches((byte) 4));

        // Containing null
        f = new ByteRangeFilter("A", NULL_BYTE, (byte) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_BYTE));
        assertTrue(f.overlaps(NULL_BYTE, NULL_BYTE));

        // Not containing null
        f = new ByteRangeFilter("A", NULL_BYTE, (byte) 4, false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_BYTE));
        assertFalse(f.overlaps(NULL_BYTE, NULL_BYTE));

        // Not containing null, normal
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_BYTE));

        f = new ByteRangeFilter("A", NULL_BYTE, NULL_BYTE, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_BYTE));
        assertTrue(f.overlaps(NULL_BYTE, NULL_BYTE));

        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_BYTE, (byte) 1)); // before
        assertTrue(f.overlaps(NULL_BYTE, (byte) 2)); // touches lower bound
    }

    public void testShortRangeFilterOverlap() {
        ShortRangeFilter f;

        // No overlap, f < range
        f = new ShortRangeFilter("A", (short) 1, (short) 2, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps((short) 3, (short) 4));

        // No overlap, f > range
        f = new ShortRangeFilter("A", (short) 3, (short) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps((short) 1, (short) 2));

        // No overlap, f ends at range
        f = new ShortRangeFilter("A", (short) 1, (short) 2, true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps((short) 2, (short) 3));

        // Overlap, f ends at range but both inclusive
        f = new ShortRangeFilter("A", (short) 1, (short) 2, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((short) 2, (short) 3));

        // Overlap, range ends at f but both inclusive
        f = new ShortRangeFilter("A", (short) 2, (short) 3, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((short) 1, (short) 2));

        // Overlap, f entirely inside range
        f = new ShortRangeFilter("A", (short) 2, (short) 3, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((short) 1, (short) 4));

        // Overlap, range entirely inside f
        f = new ShortRangeFilter("A", (short) 1, (short) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps((short) 2, (short) 3));

        // Not containing, val < f
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.matches((short) 1));

        // Not containing, val > f
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.matches((short) 5));

        // Containing, middle
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches((short) 3));

        // Containing, start
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches((short) 2));

        // Containing, end
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches((short) 4));

        // Containing, middle
        f = new ShortRangeFilter("A", (short) 2, (short) 4, false, false);
        f.initChunkFilter();
        assertTrue(f.matches((short) 3));

        // Not containing, start
        f = new ShortRangeFilter("A", (short) 2, (short) 4, false, false);
        f.initChunkFilter();
        assertFalse(f.matches((short) 2));

        // Not containing, end
        f = new ShortRangeFilter("A", (short) 2, (short) 4, false, false);
        f.initChunkFilter();
        assertFalse(f.matches((short) 4));

        // Containing null
        f = new ShortRangeFilter("A", NULL_SHORT, (short) 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_SHORT));
        assertTrue(f.overlaps(NULL_SHORT, NULL_SHORT));

        // Not containing null
        f = new ShortRangeFilter("A", NULL_SHORT, (short) 4, false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_SHORT));
        assertFalse(f.overlaps(NULL_SHORT, NULL_SHORT));

        // Not containing null, normal
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_SHORT));

        f = new ShortRangeFilter("A", NULL_SHORT, NULL_SHORT, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_SHORT));
        assertTrue(f.overlaps(NULL_SHORT, NULL_SHORT));

        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_SHORT, (short) 1)); // before
        assertTrue(f.overlaps(NULL_SHORT, (short) 2)); // touches lower bound
    }

    public void testIntRangeFilterOverlap() {
        IntRangeFilter f;

        // No overlap, f < range
        f = new IntRangeFilter("A", 1, 2, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(3, 4));

        // No overlap, f > range
        f = new IntRangeFilter("A", 3, 4, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(1, 2));

        // No overlap, f ends at range
        f = new IntRangeFilter("A", 1, 2, true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(2, 3));

        // Overlap, f ends at range but both inclusive
        f = new IntRangeFilter("A", 1, 2, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2, 3));

        // Overlap, range ends at f but both inclusive
        f = new IntRangeFilter("A", 2, 3, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1, 2));

        // Overlap, f entirely inside range
        f = new IntRangeFilter("A", 2, 3, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1, 4));

        // Overlap, range entirely inside f
        f = new IntRangeFilter("A", 1, 4, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2, 3));

        // Not containing, val < f
        f = new IntRangeFilter("A", 2, 4, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(1));

        // Not containing, val > f
        f = new IntRangeFilter("A", 2, 4, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(5));

        // Containing, middle
        f = new IntRangeFilter("A", 2, 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(3));

        // Containing, start
        f = new IntRangeFilter("A", 2, 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(2));

        // Containing, end
        f = new IntRangeFilter("A", 2, 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(4));

        // Containing, middle
        f = new IntRangeFilter("A", 2, 4, false, false);
        f.initChunkFilter();
        assertTrue(f.matches(3));

        // Not containing, start
        f = new IntRangeFilter("A", 2, 4, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(2));

        // Not containing, end
        f = new IntRangeFilter("A", 2, 4, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(4));

        // Containing null
        f = new IntRangeFilter("A", NULL_INT, 4, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_INT));
        assertTrue(f.overlaps(NULL_INT, NULL_INT));

        // Not containing null
        f = new IntRangeFilter("A", NULL_INT, 4, false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_INT));
        assertFalse(f.overlaps(NULL_INT, NULL_INT));

        // Not containing null, normal
        f = new IntRangeFilter("A", 2, 4, true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_INT));

        f = new IntRangeFilter("A", NULL_INT, NULL_INT, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_INT));
        assertTrue(f.overlaps(NULL_INT, NULL_INT));

        f = new IntRangeFilter("A", 2, 4, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_INT, 1)); // before
        assertTrue(f.overlaps(NULL_INT, 2)); // touches lower bound
    }

    public void testLongRangeFilterOverlap() {
        LongRangeFilter f;

        // No overlap, f < range
        f = new LongRangeFilter("A", 1L, 2L, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(3L, 4L));

        // No overlap, f > range
        f = new LongRangeFilter("A", 3L, 4L, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(1L, 2L));

        // No overlap, f ends at range
        f = new LongRangeFilter("A", 1L, 2L, true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(2L, 3L));

        // Overlap, f ends at range but both inclusive
        f = new LongRangeFilter("A", 1L, 2L, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2L, 3L));

        // Overlap, range ends at f but both inclusive
        f = new LongRangeFilter("A", 2L, 3L, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1L, 2L));

        // Overlap, f entirely inside range
        f = new LongRangeFilter("A", 2L, 3L, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1L, 4L));

        // Overlap, range entirely inside f
        f = new LongRangeFilter("A", 1L, 4L, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2L, 3L));

        // Not containing, val < f
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(1L));

        // Not containing, val > f
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(5L));

        // Containing, middle
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(3L));

        // Containing, start
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(2L));

        // Containing, end
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(4L));

        // Containing, middle
        f = new LongRangeFilter("A", 2L, 4L, false, false);
        f.initChunkFilter();
        assertTrue(f.matches(3L));

        // Not containing, start
        f = new LongRangeFilter("A", 2L, 4L, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(2L));

        // Not containing, end
        f = new LongRangeFilter("A", 2L, 4L, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(4L));

        // Containing null
        f = new LongRangeFilter("A", NULL_LONG, 4L, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_LONG));
        assertTrue(f.overlaps(NULL_LONG, NULL_LONG));

        // Not containing null
        f = new LongRangeFilter("A", NULL_LONG, 4L, false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_LONG));
        assertFalse(f.overlaps(NULL_LONG, NULL_LONG));

        // Not containing null, normal
        f = new LongRangeFilter("A", 2L, 4L, true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_LONG));

        f = new LongRangeFilter("A", NULL_LONG, NULL_LONG, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_LONG));
        assertTrue(f.overlaps(NULL_LONG, NULL_LONG));

        f = new LongRangeFilter("A", 2L, 4L, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_LONG, 1L)); // before
        assertTrue(f.overlaps(NULL_LONG, 2L)); // touches lower bound
    }

    public void testFloatRangeFilterOverlap() {
        FloatRangeFilter f;

        // No overlap, f < range
        f = new FloatRangeFilter("A", 1.0f, 2.0f, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(3.0f, 4.0f));

        // No overlap, f > range
        f = new FloatRangeFilter("A", 3.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(1.0f, 2.0f));

        // No overlap, f ends at range
        f = new FloatRangeFilter("A", 1.0f, 2.0f, true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(2.0f, 3.0f));

        // Overlap, f ends at range but both inclusive
        f = new FloatRangeFilter("A", 1.0f, 2.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2.0f, 3.0f));

        // Overlap, range ends at f but both inclusive
        f = new FloatRangeFilter("A", 2.0f, 3.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1.0f, 2.0f));

        // Overlap, f entirely inside range
        f = new FloatRangeFilter("A", 2.0f, 3.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1.0f, 4.0f));

        // Overlap, range entirely inside f
        f = new FloatRangeFilter("A", 1.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2.0f, 3.0f));

        // Not containing, val < f
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(1.0f));

        // Not containing, val > f
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(5.0f));

        // Containing, middle
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(3.0f));

        // Containing, start
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(2.0f));

        // Containing, end
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(4.0f));

        // Containing, middle
        f = new FloatRangeFilter("A", 2.0f, 4.0f, false, false);
        f.initChunkFilter();
        assertTrue(f.matches(3.0f));

        // Not containing, start
        f = new FloatRangeFilter("A", 2.0f, 4.0f, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(2.0f));

        // Not containing, end
        f = new FloatRangeFilter("A", 2.0f, 4.0f, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(4.0f));

        // Containing null
        f = new FloatRangeFilter("A", NULL_FLOAT, 4.0f, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_FLOAT));
        assertTrue(f.overlaps(NULL_FLOAT, NULL_FLOAT));

        // Not containing null
        f = new FloatRangeFilter("A", NULL_FLOAT, 4.0f, false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_FLOAT));
        assertFalse(f.overlaps(NULL_FLOAT, NULL_FLOAT));

        // Not containing null, normal
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_FLOAT));

        f = new FloatRangeFilter("A", NULL_FLOAT, NULL_FLOAT, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_FLOAT));
        assertTrue(f.overlaps(NULL_FLOAT, NULL_FLOAT));

        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_FLOAT, 1.0f)); // before
        assertTrue(f.overlaps(NULL_FLOAT, 2.0f)); // touches lower bound
    }

    public void testDoubleRangeFilterOverlap() {
        DoubleRangeFilter f;

        // No overlap, f < range
        f = new DoubleRangeFilter("A", 1.0, 2.0, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(3.0, 4.0));

        // No overlap, f > range
        f = new DoubleRangeFilter("A", 3.0, 4.0, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(1.0, 2.0));

        // No overlap, f ends at range
        f = new DoubleRangeFilter("A", 1.0, 2.0, true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(2.0, 3.0));

        // Overlap, f ends at range but both inclusive
        f = new DoubleRangeFilter("A", 1.0, 2.0, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2.0, 3.0));

        // Overlap, range ends at f but both inclusive
        f = new DoubleRangeFilter("A", 2.0, 3.0, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1.0, 2.0));

        // Overlap, f entirely inside range
        f = new DoubleRangeFilter("A", 2.0, 3.0, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(1.0, 4.0));

        // Overlap, range entirely inside f
        f = new DoubleRangeFilter("A", 1.0, 4.0, true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(2.0, 3.0));

        // Not containing, val < f
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(1.0));

        // Not containing, val > f
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        f.initChunkFilter();
        assertFalse(f.matches(5.0));

        // Containing, middle
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(3.0));

        // Containing, start
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(2.0));

        // Containing, end
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(4.0));

        // Containing, middle
        f = new DoubleRangeFilter("A", 2.0, 4.0, false, false);
        f.initChunkFilter();
        assertTrue(f.matches(3.0));

        // Not containing, start
        f = new DoubleRangeFilter("A", 2.0, 4.0, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(2.0));

        // Not containing, end
        f = new DoubleRangeFilter("A", 2.0, 4.0, false, false);
        f.initChunkFilter();
        assertFalse(f.matches(4.0));

        // Containing null
        f = new DoubleRangeFilter("A", NULL_DOUBLE, 4.0, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_DOUBLE));
        assertTrue(f.overlaps(NULL_DOUBLE, NULL_DOUBLE));

        // Not containing null
        f = new DoubleRangeFilter("A", NULL_DOUBLE, 4.0, false, true);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_DOUBLE));
        assertFalse(f.overlaps(NULL_DOUBLE, NULL_DOUBLE));

        // Not containing null, normal
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, false);
        f.initChunkFilter();
        assertFalse(f.matches(NULL_DOUBLE));

        f = new DoubleRangeFilter("A", NULL_DOUBLE, NULL_DOUBLE, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(NULL_DOUBLE));
        assertTrue(f.overlaps(NULL_DOUBLE, NULL_DOUBLE));

        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(NULL_DOUBLE, 1.0)); // before
        assertTrue(f.overlaps(NULL_DOUBLE, 2.0)); // touches lower bound
    }

    public void testComparableRangeFilterOverlap() {
        ComparableRangeFilter f;

        // No overlap, f < range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(3), BigDecimal.valueOf(4)));

        // No overlap, f > range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(3), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2)));

        // No overlap, f ends at range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3)));

        // Overlap, f ends at range but both inclusive
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3)));

        // Overlap, range ends at f but both inclusive
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2)));

        // Overlap, f entirely inside range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(4)));

        // Overlap, range entirely inside f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3)));

        // Not containing, val < f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(1)));

        // Not containing, val > f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(5)));

        // Containing, middle
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(3)));

        // Containing, start
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(2)));

        // Containing, end
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(4)));

        // Containing, middle
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), false, false);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(3)));

        // Not containing, start
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), false, false);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(2)));

        // Not containing, end
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), false, false);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(4)));

        // Containing null
        f = new ComparableRangeFilter("A", null, BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertTrue(f.matches(null));
        assertTrue(f.overlaps(null, null));

        // Not containing null
        f = new ComparableRangeFilter("A", null, BigDecimal.valueOf(4), false, true);
        f.initChunkFilter();
        assertFalse(f.matches(null));
        assertFalse(f.overlaps(null, null));

        // Not containing null, normal
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, false);
        f.initChunkFilter();
        assertFalse(f.matches(null));

        f = new ComparableRangeFilter("A", null, null, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(null));
        assertTrue(f.overlaps(null, null));

        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(null, BigDecimal.valueOf(1))); // before
        assertTrue(f.overlaps(null, BigDecimal.valueOf(2))); // touches lower bound
    }

    public void testSingleSidedComparableRangeFilterOverlap() {
        SingleSidedComparableRangeFilter f;

        // No overlap, f < value
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(3), BigDecimal.valueOf(4)));

        // No overlap, f > value
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(3), true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2)));

        // No overlap, f ends at value
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, false);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3)));

        // No overlap, value ends at f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2)));

        // Overlap, f ends at value but both inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, false);
        f.initChunkFilter();
        assertTrue(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3)));

        // Overlap, value ends at f but both inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertTrue(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2)));

        // Containing, value > f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(3)));

        // Not containing, value < f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(1)));

        // Containing, value == f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(2)));

        // Not containing, value == f, not inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, true);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(2)));

        // Containing, value == f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, false);
        f.initChunkFilter();
        assertTrue(f.matches(BigDecimal.valueOf(2)));

        // Not containing, value == f, not inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, false);
        f.initChunkFilter();
        assertFalse(f.matches(BigDecimal.valueOf(2)));

        // Containing null
        f = new SingleSidedComparableRangeFilter("A", null, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(null));
        assertTrue(f.overlaps(null, null));

        // Containing null
        f = new SingleSidedComparableRangeFilter("A", null, true, false);
        f.initChunkFilter();
        assertTrue(f.matches(null));
        assertTrue(f.overlaps(null, null));

        // Not containing null (since null < BD(2)
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, true);
        f.initChunkFilter();
        assertFalse(f.matches(null));

        // Containing null (since null < BD(2) )
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, false);
        f.initChunkFilter();
        assertTrue(f.matches(null));

        f = new SingleSidedComparableRangeFilter("A", null, true, true);
        f.initChunkFilter();
        assertTrue(f.matches(null));
        assertTrue(f.overlaps(null, null));

        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        f.initChunkFilter();
        assertFalse(f.overlaps(null, BigDecimal.valueOf(1))); // before
        assertTrue(f.overlaps(null, BigDecimal.valueOf(2))); // touches lower bound
    }
}
