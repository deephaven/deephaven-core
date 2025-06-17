//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.*;

public class TestRangeFilterOverlap extends RefreshingTableTestCase {

    public void testCharRangeFilterOverlap() {
        CharRangeFilter f;

        // No overlap, f < range
        f = new CharRangeFilter("A", 'a', 'b', true, true);
        assertFalse(f.overlaps('c', 'd', true, true));

        // No overlap, f > range
        f = new CharRangeFilter("A", 'c', 'd', true, true);
        assertFalse(f.overlaps('a', 'b', true, true));

        // No overlap, f ends at range
        f = new CharRangeFilter("A", 'a', 'b', true, false);
        assertFalse(f.overlaps('b', 'c', true, true));

        // No overlap, range ends at f
        f = new CharRangeFilter("A", 'b', 'c', true, true);
        assertFalse(f.overlaps('a', 'b', true, false));

        // Overlap, f ends at range but both inclusive
        f = new CharRangeFilter("A", 'a', 'b', true, true);
        assertTrue(f.overlaps('b', 'c', true, true));

        // Overlap, range ends at f but both inclusive
        f = new CharRangeFilter("A", 'b', 'c', true, true);
        assertTrue(f.overlaps('a', 'b', true, true));

        // Overlap, f entirely inside range
        f = new CharRangeFilter("A", 'b', 'c', true, true);
        assertTrue(f.overlaps('a', 'd', true, true));

        // Overlap, range entirely inside f
        f = new CharRangeFilter("A", 'a', 'd', true, true);
        assertTrue(f.overlaps('b', 'c', true, true));

        // Not containing, val < f
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        assertFalse(f.contains('a'));

        // Not containing, val > f
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        assertFalse(f.contains('e'));

        // Containing, middle
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        assertTrue(f.contains('c'));

        // Containing, start
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        assertTrue(f.contains('b'));

        // Containing, end
        f = new CharRangeFilter("A", 'b', 'd', true, true);
        assertTrue(f.contains('d'));

        // Containing, middle
        f = new CharRangeFilter("A", 'b', 'd', false, false);
        assertTrue(f.contains('c'));

        // Not containing, start
        f = new CharRangeFilter("A", 'b', 'd', false, false);
        assertFalse(f.contains('b'));

        // Not containing, end
        f = new CharRangeFilter("A", 'b', 'd', false, false);
        assertFalse(f.contains('d'));

        // Containing null
        f = new CharRangeFilter("A", NULL_CHAR, 'd', true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new CharRangeFilter("A", NULL_CHAR, 'd', false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new CharRangeFilter("A", 'b', 'd', true, false);
        assertFalse(f.containsNull());

        f = new CharRangeFilter("A", 'b', 'd', true, true);
        // (-INF,'a'] before filter
        assertFalse(f.overlaps(null, 'a', true, true));
        // Overlaps at 'b'
        assertTrue(f.overlaps(null, 'b', true, true));
    }

    public void testByteRangeFilterOverlap() {
        ByteRangeFilter f;

        // No overlap, f < range
        f = new ByteRangeFilter("A", (byte) 1, (byte) 2, true, true);
        assertFalse(f.overlaps((byte) 3, (byte) 4, true, true));

        // No overlap, f > range
        f = new ByteRangeFilter("A", (byte) 3, (byte) 4, true, true);
        assertFalse(f.overlaps((byte) 1, (byte) 2, true, true));

        // No overlap, f ends at range
        f = new ByteRangeFilter("A", (byte) 1, (byte) 2, true, false);
        assertFalse(f.overlaps((byte) 2, (byte) 3, true, true));

        // No overlap, range ends at f
        f = new ByteRangeFilter("A", (byte) 2, (byte) 3, true, true);
        assertFalse(f.overlaps((byte) 1, (byte) 2, true, false));

        // Overlap, f ends at range but both inclusive
        f = new ByteRangeFilter("A", (byte) 1, (byte) 2, true, true);
        assertTrue(f.overlaps((byte) 2, (byte) 3, true, true));

        // Overlap, range ends at f but both inclusive
        f = new ByteRangeFilter("A", (byte) 2, (byte) 3, true, true);
        assertTrue(f.overlaps((byte) 1, (byte) 2, true, true));

        // Overlap, f entirely inside range
        f = new ByteRangeFilter("A", (byte) 2, (byte) 3, true, true);
        assertTrue(f.overlaps((byte) 1, (byte) 4, true, true));

        // Overlap, range entirely inside f
        f = new ByteRangeFilter("A", (byte) 1, (byte) 4, true, true);
        assertTrue(f.overlaps((byte) 2, (byte) 3, true, true));

        // Not containing, val < f
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        assertFalse(f.contains((byte) 1));

        // Not containing, val > f
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        assertFalse(f.contains((byte) 5));

        // Containing, middle
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        assertTrue(f.contains((byte) 3));

        // Containing, start
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        assertTrue(f.contains((byte) 2));

        // Containing, end
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        assertTrue(f.contains((byte) 4));

        // Containing, middle
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, false, false);
        assertTrue(f.contains((byte) 3));

        // Not containing, start
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, false, false);
        assertFalse(f.contains((byte) 2));

        // Not containing, end
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, false, false);
        assertFalse(f.contains((byte) 4));

        // Containing null
        f = new ByteRangeFilter("A", NULL_BYTE, (byte) 4, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new ByteRangeFilter("A", NULL_BYTE, (byte) 4, false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, false);
        assertFalse(f.containsNull());

        f = new ByteRangeFilter("A", (byte) 2, (byte) 4, true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, (byte) 1, true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, (byte) 2, true, true));
    }

    public void testShortRangeFilterOverlap() {
        ShortRangeFilter f;

        // No overlap, f < range
        f = new ShortRangeFilter("A", (short) 1, (short) 2, true, true);
        assertFalse(f.overlaps((short) 3, (short) 4, true, true));

        // No overlap, f > range
        f = new ShortRangeFilter("A", (short) 3, (short) 4, true, true);
        assertFalse(f.overlaps((short) 1, (short) 2, true, true));

        // No overlap, f ends at range
        f = new ShortRangeFilter("A", (short) 1, (short) 2, true, false);
        assertFalse(f.overlaps((short) 2, (short) 3, true, true));

        // No overlap, range ends at f
        f = new ShortRangeFilter("A", (short) 2, (short) 3, true, true);
        assertFalse(f.overlaps((short) 1, (short) 2, true, false));

        // Overlap, f ends at range but both inclusive
        f = new ShortRangeFilter("A", (short) 1, (short) 2, true, true);
        assertTrue(f.overlaps((short) 2, (short) 3, true, true));

        // Overlap, range ends at f but both inclusive
        f = new ShortRangeFilter("A", (short) 2, (short) 3, true, true);
        assertTrue(f.overlaps((short) 1, (short) 2, true, true));

        // Overlap, f entirely inside range
        f = new ShortRangeFilter("A", (short) 2, (short) 3, true, true);
        assertTrue(f.overlaps((short) 1, (short) 4, true, true));

        // Overlap, range entirely inside f
        f = new ShortRangeFilter("A", (short) 1, (short) 4, true, true);
        assertTrue(f.overlaps((short) 2, (short) 3, true, true));

        // Not containing, val < f
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        assertFalse(f.contains((short) 1));

        // Not containing, val > f
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        assertFalse(f.contains((short) 5));

        // Containing, middle
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        assertTrue(f.contains((short) 3));

        // Containing, start
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        assertTrue(f.contains((short) 2));

        // Containing, end
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        assertTrue(f.contains((short) 4));

        // Containing, middle
        f = new ShortRangeFilter("A", (short) 2, (short) 4, false, false);
        assertTrue(f.contains((short) 3));

        // Not containing, start
        f = new ShortRangeFilter("A", (short) 2, (short) 4, false, false);
        assertFalse(f.contains((short) 2));

        // Not containing, end
        f = new ShortRangeFilter("A", (short) 2, (short) 4, false, false);
        assertFalse(f.contains((short) 4));

        // Containing null
        f = new ShortRangeFilter("A", NULL_SHORT, (short) 4, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new ShortRangeFilter("A", NULL_SHORT, (short) 4, false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, false);
        assertFalse(f.containsNull());

        f = new ShortRangeFilter("A", (short) 2, (short) 4, true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, (short) 1, true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, (short) 2, true, true));
    }

    public void testIntRangeFilterOverlap() {
        IntRangeFilter f;

        // No overlap, f < range
        f = new IntRangeFilter("A", 1, 2, true, true);
        assertFalse(f.overlaps(3, 4, true, true));

        // No overlap, f > range
        f = new IntRangeFilter("A", 3, 4, true, true);
        assertFalse(f.overlaps(1, 2, true, true));

        // No overlap, f ends at range
        f = new IntRangeFilter("A", 1, 2, true, false);
        assertFalse(f.overlaps(2, 3, true, true));

        // No overlap, range ends at f
        f = new IntRangeFilter("A", 2, 3, true, true);
        assertFalse(f.overlaps(1, 2, true, false));

        // Overlap, f ends at range but both inclusive
        f = new IntRangeFilter("A", 1, 2, true, true);
        assertTrue(f.overlaps(2, 3, true, true));

        // Overlap, range ends at f but both inclusive
        f = new IntRangeFilter("A", 2, 3, true, true);
        assertTrue(f.overlaps(1, 2, true, true));

        // Overlap, f entirely inside range
        f = new IntRangeFilter("A", 2, 3, true, true);
        assertTrue(f.overlaps(1, 4, true, true));

        // Overlap, range entirely inside f
        f = new IntRangeFilter("A", 1, 4, true, true);
        assertTrue(f.overlaps(2, 3, true, true));

        // Not containing, val < f
        f = new IntRangeFilter("A", 2, 4, true, true);
        assertFalse(f.contains(1));

        // Not containing, val > f
        f = new IntRangeFilter("A", 2, 4, true, true);
        assertFalse(f.contains(5));

        // Containing, middle
        f = new IntRangeFilter("A", 2, 4, true, true);
        assertTrue(f.contains(3));

        // Containing, start
        f = new IntRangeFilter("A", 2, 4, true, true);
        assertTrue(f.contains(2));

        // Containing, end
        f = new IntRangeFilter("A", 2, 4, true, true);
        assertTrue(f.contains(4));

        // Containing, middle
        f = new IntRangeFilter("A", 2, 4, false, false);
        assertTrue(f.contains(3));

        // Not containing, start
        f = new IntRangeFilter("A", 2, 4, false, false);
        assertFalse(f.contains(2));

        // Not containing, end
        f = new IntRangeFilter("A", 2, 4, false, false);
        assertFalse(f.contains(4));

        // Containing null
        f = new IntRangeFilter("A", NULL_INT, 4, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new IntRangeFilter("A", NULL_INT, 4, false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new IntRangeFilter("A", 2, 4, true, false);
        assertFalse(f.containsNull());

        f = new IntRangeFilter("A", 2, 4, true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, 1, true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, 2, true, true));
    }

    public void testLongRangeFilterOverlap() {
        LongRangeFilter f;

        // No overlap, f < range
        f = new LongRangeFilter("A", 1L, 2L, true, true);
        assertFalse(f.overlaps(3L, 4L, true, true));

        // No overlap, f > range
        f = new LongRangeFilter("A", 3L, 4L, true, true);
        assertFalse(f.overlaps(1L, 2L, true, true));

        // No overlap, f ends at range
        f = new LongRangeFilter("A", 1L, 2L, true, false);
        assertFalse(f.overlaps(2L, 3L, true, true));

        // No overlap, range ends at f
        f = new LongRangeFilter("A", 2L, 3L, true, true);
        assertFalse(f.overlaps(1L, 2L, true, false));

        // Overlap, f ends at range but both inclusive
        f = new LongRangeFilter("A", 1L, 2L, true, true);
        assertTrue(f.overlaps(2L, 3L, true, true));

        // Overlap, range ends at f but both inclusive
        f = new LongRangeFilter("A", 2L, 3L, true, true);
        assertTrue(f.overlaps(1L, 2L, true, true));

        // Overlap, f entirely inside range
        f = new LongRangeFilter("A", 2L, 3L, true, true);
        assertTrue(f.overlaps(1L, 4L, true, true));

        // Overlap, range entirely inside f
        f = new LongRangeFilter("A", 1L, 4L, true, true);
        assertTrue(f.overlaps(2L, 3L, true, true));

        // Not containing, val < f
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        assertFalse(f.contains(1L));

        // Not containing, val > f
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        assertFalse(f.contains(5L));

        // Containing, middle
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        assertTrue(f.contains(3L));

        // Containing, start
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        assertTrue(f.contains(2L));

        // Containing, end
        f = new LongRangeFilter("A", 2L, 4L, true, true);
        assertTrue(f.contains(4L));

        // Containing, middle
        f = new LongRangeFilter("A", 2L, 4L, false, false);
        assertTrue(f.contains(3L));

        // Not containing, start
        f = new LongRangeFilter("A", 2L, 4L, false, false);
        assertFalse(f.contains(2L));

        // Not containing, end
        f = new LongRangeFilter("A", 2L, 4L, false, false);
        assertFalse(f.contains(4L));

        // Containing null
        f = new LongRangeFilter("A", NULL_LONG, 4L, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new LongRangeFilter("A", NULL_LONG, 4L, false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new LongRangeFilter("A", 2L, 4L, true, false);
        assertFalse(f.containsNull());

        f = new LongRangeFilter("A", 2L, 4L, true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, 1L, true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, 2L, true, true));
    }

    public void testFloatRangeFilterOverlap() {
        FloatRangeFilter f;

        // No overlap, f < range
        f = new FloatRangeFilter("A", 1.0f, 2.0f, true, true);
        assertFalse(f.overlaps(3.0f, 4.0f, true, true));

        // No overlap, f > range
        f = new FloatRangeFilter("A", 3.0f, 4.0f, true, true);
        assertFalse(f.overlaps(1.0f, 2.0f, true, true));

        // No overlap, f ends at range
        f = new FloatRangeFilter("A", 1.0f, 2.0f, true, false);
        assertFalse(f.overlaps(2.0f, 3.0f, true, true));

        // No overlap, range ends at f
        f = new FloatRangeFilter("A", 2.0f, 3.0f, true, true);
        assertFalse(f.overlaps(1.0f, 2.0f, true, false));

        // Overlap, f ends at range but both inclusive
        f = new FloatRangeFilter("A", 1.0f, 2.0f, true, true);
        assertTrue(f.overlaps(2.0f, 3.0f, true, true));

        // Overlap, range ends at f but both inclusive
        f = new FloatRangeFilter("A", 2.0f, 3.0f, true, true);
        assertTrue(f.overlaps(1.0f, 2.0f, true, true));

        // Overlap, f entirely inside range
        f = new FloatRangeFilter("A", 2.0f, 3.0f, true, true);
        assertTrue(f.overlaps(1.0f, 4.0f, true, true));

        // Overlap, range entirely inside f
        f = new FloatRangeFilter("A", 1.0f, 4.0f, true, true);
        assertTrue(f.overlaps(2.0f, 3.0f, true, true));

        // Not containing, val < f
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        assertFalse(f.contains(1.0f));

        // Not containing, val > f
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        assertFalse(f.contains(5.0f));

        // Containing, middle
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        assertTrue(f.contains(3.0f));

        // Containing, start
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        assertTrue(f.contains(2.0f));

        // Containing, end
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        assertTrue(f.contains(4.0f));

        // Containing, middle
        f = new FloatRangeFilter("A", 2.0f, 4.0f, false, false);
        assertTrue(f.contains(3.0f));

        // Not containing, start
        f = new FloatRangeFilter("A", 2.0f, 4.0f, false, false);
        assertFalse(f.contains(2.0f));

        // Not containing, end
        f = new FloatRangeFilter("A", 2.0f, 4.0f, false, false);
        assertFalse(f.contains(4.0f));

        // Containing null
        f = new FloatRangeFilter("A", NULL_FLOAT, 4.0f, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new FloatRangeFilter("A", NULL_FLOAT, 4.0f, false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, false);
        assertFalse(f.containsNull());

        f = new FloatRangeFilter("A", 2.0f, 4.0f, true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, 1.0f, true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, 2.0f, true, true));
    }

    public void testDoubleRangeFilterOverlap() {
        DoubleRangeFilter f;

        // No overlap, f < range
        f = new DoubleRangeFilter("A", 1.0, 2.0, true, true);
        assertFalse(f.overlaps(3.0, 4.0, true, true));

        // No overlap, f > range
        f = new DoubleRangeFilter("A", 3.0, 4.0, true, true);
        assertFalse(f.overlaps(1.0, 2.0, true, true));

        // No overlap, f ends at range
        f = new DoubleRangeFilter("A", 1.0, 2.0, true, false);
        assertFalse(f.overlaps(2.0, 3.0, true, true));

        // No overlap, range ends at f
        f = new DoubleRangeFilter("A", 2.0, 3.0, true, true);
        assertFalse(f.overlaps(1.0, 2.0, true, false));

        // Overlap, f ends at range but both inclusive
        f = new DoubleRangeFilter("A", 1.0, 2.0, true, true);
        assertTrue(f.overlaps(2.0, 3.0, true, true));

        // Overlap, range ends at f but both inclusive
        f = new DoubleRangeFilter("A", 2.0, 3.0, true, true);
        assertTrue(f.overlaps(1.0, 2.0, true, true));

        // Overlap, f entirely inside range
        f = new DoubleRangeFilter("A", 2.0, 3.0, true, true);
        assertTrue(f.overlaps(1.0, 4.0, true, true));

        // Overlap, range entirely inside f
        f = new DoubleRangeFilter("A", 1.0, 4.0, true, true);
        assertTrue(f.overlaps(2.0, 3.0, true, true));

        // Not containing, val < f
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        assertFalse(f.contains(1.0));

        // Not containing, val > f
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        assertFalse(f.contains(5.0));

        // Containing, middle
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        assertTrue(f.contains(3.0));

        // Containing, start
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        assertTrue(f.contains(2.0));

        // Containing, end
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        assertTrue(f.contains(4.0));

        // Containing, middle
        f = new DoubleRangeFilter("A", 2.0, 4.0, false, false);
        assertTrue(f.contains(3.0));

        // Not containing, start
        f = new DoubleRangeFilter("A", 2.0, 4.0, false, false);
        assertFalse(f.contains(2.0));

        // Not containing, end
        f = new DoubleRangeFilter("A", 2.0, 4.0, false, false);
        assertFalse(f.contains(4.0));

        // Containing null
        f = new DoubleRangeFilter("A", NULL_DOUBLE, 4.0, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new DoubleRangeFilter("A", NULL_DOUBLE, 4.0, false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new DoubleRangeFilter("A", 2.0, 4.0, true, false);
        assertFalse(f.containsNull());

        f = new DoubleRangeFilter("A", 2.0, 4.0, true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, 1.0, true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, 2.0, true, true));
    }

    public void testComparableRangeFilterOverlap() {
        ComparableRangeFilter f;

        // No overlap, f < range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true);
        assertFalse(f.overlaps(BigDecimal.valueOf(3), BigDecimal.valueOf(4), true, true));

        // No overlap, f > range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(3), BigDecimal.valueOf(4), true, true);
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true));

        // No overlap, f ends at range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, false);
        assertFalse(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true));

        // No overlap, range ends at f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true);
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, false));

        // Overlap, f ends at range but both inclusive
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true);
        assertTrue(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true));

        // Overlap, range ends at f but both inclusive
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true);
        assertTrue(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true));

        // Overlap, f entirely inside range
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true);
        assertTrue(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(4), true, true));

        // Overlap, range entirely inside f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(1), BigDecimal.valueOf(4), true, true);
        assertTrue(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true));

        // Not containing, val < f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        assertFalse(f.contains(BigDecimal.valueOf(1)));

        // Not containing, val > f
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        assertFalse(f.contains(BigDecimal.valueOf(5)));

        // Containing, middle
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        assertTrue(f.contains(BigDecimal.valueOf(3)));

        // Containing, start
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        assertTrue(f.contains(BigDecimal.valueOf(2)));

        // Containing, end
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, true);
        assertTrue(f.contains(BigDecimal.valueOf(4)));

        // Containing, middle
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), false, false);
        assertTrue(f.contains(BigDecimal.valueOf(3)));

        // Not containing, start
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), false, false);
        assertFalse(f.contains(BigDecimal.valueOf(2)));

        // Not containing, end
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), false, false);
        assertFalse(f.contains(BigDecimal.valueOf(4)));

        // Containing null
        f = new ComparableRangeFilter("A", null, BigDecimal.valueOf(4), true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null
        f = new ComparableRangeFilter("A", null, BigDecimal.valueOf(4), false, true);
        assertFalse(f.containsNull());
        assertFalse(f.overlaps(null, null, true, true));

        // Not containing null, normal
        f = new ComparableRangeFilter("A", BigDecimal.valueOf(2), BigDecimal.valueOf(4), true, false);
        assertFalse(f.containsNull());
    }

    public void testSingleSidedComparableRangeFilterOverlap() {
        SingleSidedComparableRangeFilter f;

        // No overlap, f < value
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, false);
        assertFalse(f.overlaps(BigDecimal.valueOf(3), BigDecimal.valueOf(4), true, true));

        // No overlap, f > value
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(3), true, true);
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true));

        // No overlap, f ends at value
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, false);
        assertFalse(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true));

        // No overlap, value ends at f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, true);
        assertFalse(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true));

        // Overlap, f ends at value but both inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, false);
        assertTrue(f.overlaps(BigDecimal.valueOf(2), BigDecimal.valueOf(3), true, true));

        // Overlap, value ends at f but both inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        assertTrue(f.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(2), true, true));

        // Containing, value > f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        assertTrue(f.contains(BigDecimal.valueOf(3)));

        // Not containing, value < f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        assertFalse(f.contains(BigDecimal.valueOf(1)));

        // Containing, value == f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        assertTrue(f.contains(BigDecimal.valueOf(2)));

        // Not containing, value == f, not inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, true);
        assertFalse(f.contains(BigDecimal.valueOf(2)));

        // Containing, value == f
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, false);
        assertTrue(f.contains(BigDecimal.valueOf(2)));

        // Not containing, value == f, not inclusive
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, false);
        assertFalse(f.contains(BigDecimal.valueOf(2)));

        // Containing null
        f = new SingleSidedComparableRangeFilter("A", null, true, true);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Containing null
        f = new SingleSidedComparableRangeFilter("A", null, true, false);
        assertTrue(f.containsNull());
        assertTrue(f.overlaps(null, null, true, true));

        // Not containing null (since null < BD(2)
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, true);
        assertFalse(f.containsNull());

        // Containing null (since null < BD(2) )
        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), false, false);
        assertTrue(f.containsNull());

        f = new SingleSidedComparableRangeFilter("A", BigDecimal.valueOf(2), true, true);
        // (-INF,1] before filter
        assertFalse(f.overlaps(null, BigDecimal.valueOf(1), true, true));
        // Overlaps at 2
        assertTrue(f.overlaps(null, BigDecimal.valueOf(2), true, true));
    }

    public void testMismatchedDataTypes() {
        // CharFilter with int values
        CharRangeFilter f1 = new CharRangeFilter("A", 'a', 'd', true, true);
        assertTrue(f1.overlaps((int) 'a', (int) 'f', true, true));
        assertTrue(f1.contains((int) 'c'));

        // IntFilter with char values
        IntRangeFilter f2 = new IntRangeFilter("A", 'a', 'd', true, true);
        assertTrue(f2.overlaps((int) 'a', (int) 'f', true, true));
        assertTrue(f2.contains((int) 'c'));

        // IntFilter with long values
        IntRangeFilter f3 = new IntRangeFilter("A", 4, 10, true, true);
        assertTrue(f3.overlaps(1L, 5L, true, true));
        assertTrue(f3.contains(5L));

        // LongFilter with int values
        LongRangeFilter f4 = new LongRangeFilter("A", 4L, 10L, true, true);
        assertTrue(f4.overlaps(1, 5, true, true));
        assertTrue(f4.contains(5));

        // FloatFilter with double values
        FloatRangeFilter f5 = new FloatRangeFilter("A", 4.0f, 10.0f, true, true);
        assertTrue(f5.overlaps(1.0, 5.0, true, true));
        assertTrue(f5.contains(5.0));

        // DoubleFilter with float values
        DoubleRangeFilter f6 = new DoubleRangeFilter("A", 4.0, 10.0, true, true);
        assertTrue(f6.overlaps(1.0f, 5.0f, true, true));
        assertTrue(f6.contains(5.0f));

        // ComparableFilter with BigDecimal values
        ComparableRangeFilter f7 =
                new ComparableRangeFilter("A", BigDecimal.valueOf(4), BigDecimal.valueOf(10), true, true);
        // vs. doubles
        assertTrue(f7.overlaps(1.0f, 5.0f, true, true));
        assertTrue(f7.contains(5.0f));
        // vs. long
        assertTrue(f7.overlaps(1L, 5L, true, true));
        assertTrue(f7.contains(5L));
        // vs. char
        assertTrue(f7.overlaps((char) 1, (char) 5, true, true));
        assertTrue(f7.contains((char) 5));
        // vs. BigInteger
        assertTrue(f7.overlaps(BigInteger.valueOf(1), BigInteger.valueOf(5), true, true));
        assertTrue(f7.contains(BigInteger.valueOf(5)));

        // ComparableFilter with BigInteger values
        ComparableRangeFilter f8 =
                new ComparableRangeFilter("A", BigInteger.valueOf(4), BigInteger.valueOf(10), true, true);
        // vs. doubles
        assertTrue(f8.overlaps(1.0f, 5.0f, true, true));
        assertTrue(f8.contains(5.0f));
        // vs. long
        assertTrue(f8.overlaps(1L, 5L, true, true));
        assertTrue(f8.contains(5L));
        // vs. char
        assertTrue(f8.overlaps((char) 1, (char) 5, true, true));
        assertTrue(f8.contains((char) 5));
        // vs. BigDecimal
        assertTrue(f8.overlaps(BigDecimal.valueOf(1), BigDecimal.valueOf(5), true, true));
        assertTrue(f8.contains(BigDecimal.valueOf(5)));
    }
}
