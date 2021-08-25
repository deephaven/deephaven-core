package io.deephaven.db.v2.utils.sortedranges;

import static io.deephaven.db.v2.utils.TstIndexUtil.sortedRangesFromString;
import static org.junit.Assert.*;

import io.deephaven.db.v2.utils.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

public class SortedRangesOrderedKeysTest extends OrderedKeysTestBase {
    @Override
    protected OrderedKeys create(long... values) {
        SortedRanges sar = SortedRanges.makeForKnownRange(values[0], values[values.length - 1], true);
        if (sar == null) {
            throw new IllegalStateException();
        }
        for (long v : values) {
            sar = sar.add(v);
            if (sar == null) {
                throw new IllegalStateException();
            }
        }
        return new TreeIndex(sar);
    }

    @Test
    public void testThroughForCoverage() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(0, 100);
        sr = sr.add(150);
        sr = sr.add(200);
        sr = sr.add(203);
        sr = sr.addRange(210, 215);
        sr = sr.addRange(220, 240);
        try (final OrderedKeys.Iterator it = sr.getOrderedKeysIterator()) {
            long posBefore = it.getRelativePosition();
            it.getNextOrderedKeysThrough(200);
            long posAfter = it.getRelativePosition();
            assertEquals(103, posAfter - posBefore);
            posBefore = posAfter;
            it.getNextOrderedKeysThrough(210);
            posAfter = it.getRelativePosition();
            assertEquals(2, posAfter - posBefore);
            posBefore = posAfter;
            final OrderedKeys oks = it.getNextOrderedKeysThrough(230);
            posAfter = it.getRelativePosition();
            assertEquals(16, posAfter - posBefore);
            try (final OrderedKeys.Iterator it2 = oks.getOrderedKeysIterator()) {
                final OrderedKeys oks2 = it2.getNextOrderedKeysThrough(231);
                assertEquals(16, oks2.size());
            }
        }
    }

    @Test
    public void testThroughForCoverage2() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(0, 9);
        sr = sr.add(22);
        try (final OrderedKeys.Iterator it = sr.getOrderedKeysIterator()) {
            OrderedKeys oks = it.getNextOrderedKeysThrough(9);
            assertEquals(10, oks.size());
            oks = it.getNextOrderedKeysThrough(23);
            assertEquals(1, oks.size());
            assertEquals(22, oks.lastKey());
        }
    }

    @Test
    public void testAdvanceBugSr() {
        advanceBug(TreeIndex.makeEmptySr());
    }

    @Test
    public void testNextCase0() {
        SortedRanges sr = SortedRanges.makeSingleRange(0, 10);
        sr = sr.add(12);
        sr = sr.add(14);
        sr = sr.add(16);
        final OrderedKeys ok = sr.getOrderedKeysByKeyRange(1, 14);
        try (final OrderedKeys.Iterator it = ok.getOrderedKeysIterator()) {
            final OrderedKeys ok2 = it.getNextOrderedKeysWithLength(10);
            assertEquals(10, ok2.size());
            final OrderedKeys ok3 = it.getNextOrderedKeysWithLength(10);
            assertEquals(2, ok3.size());
            final Index ix = ok3.asIndex();
            assertEquals(2, ix.size());
            assertEquals(12, ix.firstKey());
            assertEquals(14, ix.lastKey());
        }
    }

    @Test
    public void testForEach() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(0, 9);
        sr = sr.add(15);
        sr = sr.addRange(20, 29);
        try (final OrderedKeys ok = sr.getOrderedKeysByKeyRange(5, 24)) {
            final long[] expected = new long[] {5, 6, 7, 8, 9, 15, 20, 21, 22, 23, 24};
            final MutableInt i = new MutableInt(0);
            ok.forEachLong((final long v) -> {
                final int j = i.intValue();
                assertEquals("v==" + v + " && j==" + j, expected[j], v);
                i.increment();
                return true;
            });
            assertEquals(expected.length, i.intValue());
        }
    }

    @Test
    public void testOkByOne() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(0, 2);
        sr = sr.add(7);
        sr = sr.addRange(12, 14);
        final long[] expected = new long[] {0, 1, 2, 7, 12, 13, 14};
        int i = 0;
        try (final OrderedKeys.Iterator it = sr.getOrderedKeysIterator()) {
            while (it.hasMore()) {
                final OrderedKeys ok = it.getNextOrderedKeysWithLength(1);
                ((SortedRangesOrderedKeys) ok).validate();
                final String m = "i==" + i;
                assertEquals(m, 1, ok.size());
                assertEquals(m, expected[i], ok.firstKey());
                final Index ix = ok.asIndex();
                assertEquals(m, 1, ix.size());
                ++i;
            }
            assertEquals(i, expected.length);
        }
    }

    @Test
    public void testOkNextWithLengthCase0() {
        final SortedRanges sr = sortedRangesFromString(
                "0-21,23,25-32,34-38,40-43,45-48,50-63,65-66,68,70,72-73");
        assertNotNull(sr);
        for (int step = 1; step < 7; ++step) {
            final String m = "step==" + step;
            try (final OrderedKeys.Iterator okit = sr.getOrderedKeysIterator()) {
                long accum = 0;
                while (okit.hasMore()) {
                    final String m2 = m + " && accum==" + accum;
                    final OrderedKeys ok = okit.getNextOrderedKeysWithLength(step);
                    final Index expected = new TreeIndex(sr.ixSubindexByPosOnNew(accum, accum + step));
                    final Index fromOk = ok.asIndex();
                    assertEquals(m2, expected.size(), fromOk.size());
                    assertTrue(m2, expected.subsetOf(fromOk));
                    accum += step;
                }
            }
        }
    }

    @Test
    public void testSingleElementSrOk() {
        final SortedRanges sr = SortedRanges.makeSingleElement(9);
        try (final SortedRangesOrderedKeys ok = new SortedRangesOrderedKeys(sr)) {
            assertEquals(1, ok.size());
            assertEquals(9, ok.firstKey());
            assertEquals(9, ok.lastKey());
            ok.validate();
        }
    }

    @Test
    public void testOrderedKeysByPositionCases() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(1, 3);
        sr = sr.add(7);
        sr = sr.addRange(9, 12);
        try (final SortedRangesOrderedKeys ok = new SortedRangesOrderedKeys(sr);
                final SortedRangesOrderedKeys ok2 =
                        (SortedRangesOrderedKeys) ok.getOrderedKeysByPosition(3, 7);
                final SortedRangesOrderedKeys ok3 =
                        (SortedRangesOrderedKeys) ok2.getOrderedKeysByPosition(2, 5)) {
            ok.validate();
            ok2.validate();
            ok3.validate();
            assertEquals(3, ok3.size());
        }
    }
}
