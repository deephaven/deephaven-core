/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.sortedranges;

import static org.junit.Assert.*;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.RowSequenceTestBase;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.testutil.rowset.RowSetTstUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

public class SortedRangesRowSequenceTest extends RowSequenceTestBase {
    @Override
    protected RowSequence create(long... values) {
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
        return new WritableRowSetImpl(sar);
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
        try (final RowSequence.Iterator it = sr.getRowSequenceIterator()) {
            long posBefore = it.getRelativePosition();
            it.getNextRowSequenceThrough(200);
            long posAfter = it.getRelativePosition();
            assertEquals(103, posAfter - posBefore);
            posBefore = posAfter;
            it.getNextRowSequenceThrough(210);
            posAfter = it.getRelativePosition();
            assertEquals(2, posAfter - posBefore);
            posBefore = posAfter;
            final RowSequence rs = it.getNextRowSequenceThrough(230);
            posAfter = it.getRelativePosition();
            assertEquals(16, posAfter - posBefore);
            try (final RowSequence.Iterator it2 = rs.getRowSequenceIterator()) {
                final RowSequence rs2 = it2.getNextRowSequenceThrough(231);
                assertEquals(16, rs2.size());
            }
        }
    }

    @Test
    public void testThroughForCoverage2() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(0, 9);
        sr = sr.add(22);
        try (final RowSequence.Iterator it = sr.getRowSequenceIterator()) {
            RowSequence rs = it.getNextRowSequenceThrough(9);
            assertEquals(10, rs.size());
            rs = it.getNextRowSequenceThrough(23);
            assertEquals(1, rs.size());
            assertEquals(22, rs.lastRowKey());
        }
    }

    @Test
    public void testAdvanceBugSr() {
        advanceBug(RowSetTstUtils.makeEmptySr());
    }

    @Test
    public void testNextCase0() {
        SortedRanges sr = SortedRanges.makeSingleRange(0, 10);
        sr = sr.add(12);
        sr = sr.add(14);
        sr = sr.add(16);
        final RowSequence rs = sr.getRowSequenceByKeyRange(1, 14);
        try (final RowSequence.Iterator it = rs.getRowSequenceIterator()) {
            final RowSequence rs2 = it.getNextRowSequenceWithLength(10);
            assertEquals(10, rs2.size());
            final RowSequence rs3 = it.getNextRowSequenceWithLength(10);
            assertEquals(2, rs3.size());
            final RowSet ix = rs3.asRowSet();
            assertEquals(2, ix.size());
            assertEquals(12, ix.firstRowKey());
            assertEquals(14, ix.lastRowKey());
        }
    }

    @Test
    public void testForEach() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(0, 9);
        sr = sr.add(15);
        sr = sr.addRange(20, 29);
        try (final RowSequence rs = sr.getRowSequenceByKeyRange(5, 24)) {
            final long[] expected = new long[] {5, 6, 7, 8, 9, 15, 20, 21, 22, 23, 24};
            final MutableInt i = new MutableInt(0);
            rs.forEachRowKey((final long v) -> {
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
        try (final RowSequence.Iterator it = sr.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(1);
                ((SortedRangesRowSequence) rs).validate();
                final String m = "i==" + i;
                assertEquals(m, 1, rs.size());
                assertEquals(m, expected[i], rs.firstRowKey());
                final RowSet ix = rs.asRowSet();
                assertEquals(m, 1, ix.size());
                ++i;
            }
            assertEquals(i, expected.length);
        }
    }

    @Test
    public void testOkNextWithLengthCase0() {
        final SortedRanges sr = RowSetTstUtils.sortedRangesFromString(
                "0-21,23,25-32,34-38,40-43,45-48,50-63,65-66,68,70,72-73");
        assertNotNull(sr);
        for (int step = 1; step < 7; ++step) {
            final String m = "step==" + step;
            try (final RowSequence.Iterator rsIt = sr.getRowSequenceIterator()) {
                long accum = 0;
                while (rsIt.hasMore()) {
                    final String m2 = m + " && accum==" + accum;
                    final RowSequence rs = rsIt.getNextRowSequenceWithLength(step);
                    final RowSet expected =
                            new WritableRowSetImpl(sr.ixSubindexByPosOnNew(accum, accum + step));
                    final RowSet fromOk = rs.asRowSet();
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
        try (final SortedRangesRowSequence rs = new SortedRangesRowSequence(sr)) {
            assertEquals(1, rs.size());
            assertEquals(9, rs.firstRowKey());
            assertEquals(9, rs.lastRowKey());
            rs.validate();
        }
    }

    @Test
    public void testRowSequenceByPositionCases() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(1, 3);
        sr = sr.add(7);
        sr = sr.addRange(9, 12);
        try (final SortedRangesRowSequence rs = new SortedRangesRowSequence(sr);
                final SortedRangesRowSequence rs2 =
                        (SortedRangesRowSequence) rs.getRowSequenceByPosition(3, 7);
                final SortedRangesRowSequence rs3 =
                        (SortedRangesRowSequence) rs2.getRowSequenceByPosition(2, 5)) {
            rs.validate();
            rs2.validate();
            rs3.validate();
            assertEquals(3, rs3.size());
        }
    }
}
