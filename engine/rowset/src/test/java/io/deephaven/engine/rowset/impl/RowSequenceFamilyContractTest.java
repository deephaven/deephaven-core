//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.SafeCloseableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Behavior every {@link RowSequence} implementation owes, checked against all of them: the three rowset backings and
 * the two chunk-backed forms. A sequence built from a chunk of keys, a chunk of ranges, or any rowset must answer the
 * same way.
 */
public class RowSequenceFamilyContractTest {

    /**
     * Strict pool tracking, as {@link RowSequenceTestBase} uses: wrapping a chunk in a row sequence does not transfer
     * ownership of it, so a chunk this test allocates and forgets is a pool leak that would otherwise go unnoticed.
     */
    @Before
    public void setUp() {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    private static final long MAX = Long.MAX_VALUE;

    /** Names paired with builders that turn a list of ranges into a RowSequence. */
    private static final List<String> NAMES = List.of(
            "single range", "sorted ranges", "rsp", "keys chunk", "ranges chunk");

    private static List<RowSequence> sequencesFor(final long[][] ranges, final List<SafeCloseableList> junk) {
        final List<RowSequence> out = new ArrayList<>();
        final SafeCloseableList keep = new SafeCloseableList();
        junk.add(keep);

        // Rowset-backed, one per backing that can hold the shape.
        if (ranges.length == 1) {
            final WritableRowSet rs = new WritableRowSetImpl(SingleRange.make(ranges[0][0], ranges[0][1]));
            keep.add(rs);
            out.add(rs.getRowSequenceByPosition(0, rs.size()));
        } else {
            out.add(null); // single range cannot hold several disjoint ranges
        }
        SortedRanges sr = SortedRanges.makeSingleRange(ranges[0][0], ranges[0][1]);
        for (int i = 1; i < ranges.length; ++i) {
            sr = sr.addRange(ranges[i][0], ranges[i][1]);
        }
        final WritableRowSet sorted = new WritableRowSetImpl(sr);
        keep.add(sorted);
        out.add(sorted.getRowSequenceByPosition(0, sorted.size()));

        final RspBitmap rsp = RspBitmap.makeSingleRange(ranges[0][0], ranges[0][1]);
        for (int i = 1; i < ranges.length; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(ranges[i][0], ranges[i][1]);
        }
        rsp.finishMutations();
        final WritableRowSet paged = new WritableRowSetImpl(rsp);
        keep.add(paged);
        out.add(paged.getRowSequenceByPosition(0, paged.size()));

        // Chunk-backed: keys, then ranges.
        long card = 0;
        for (final long[] r : ranges) {
            card += r[1] - r[0] + 1;
        }
        if (card <= 4096) {
            // Registered with keep, not left to the sequence: wrapping a chunk does not transfer ownership of it, so
            // closing the sequence returns nothing to the chunk pool.
            final WritableLongChunk<OrderedRowKeys> keys = keep.add(WritableLongChunk.makeWritableChunk((int) card));
            int i = 0;
            for (final long[] r : ranges) {
                for (long v = r[0]; v <= r[1]; ++v) {
                    keys.set(i++, v);
                    if (v == MAX) {
                        break;
                    }
                }
            }
            keys.setSize(i);
            out.add(RowSequenceFactory.wrapRowKeysChunkAsRowSequence(keys));
        } else {
            out.add(null);
        }

        // As above: the ranges chunk stays ours to release.
        final WritableLongChunk<OrderedRowKeyRanges> rc =
                keep.add(WritableLongChunk.makeWritableChunk(2 * ranges.length));
        for (int i = 0; i < ranges.length; ++i) {
            rc.set(2 * i, ranges[i][0]);
            rc.set(2 * i + 1, ranges[i][1]);
        }
        rc.setSize(2 * ranges.length);
        out.add(RowSequenceFactory.wrapKeyRangesChunkAsRowSequence(rc));
        return out;
    }

    /** Walking a sequence whose last range ends at the last key must stop there. */
    @Test
    public void testForEachRowKeyStopsAtTheLastKey() {
        final long[][] shape = {{10, 12}, {MAX - 2, MAX}};
        final List<SafeCloseableList> junk = new ArrayList<>();
        try {
            final List<RowSequence> seqs = sequencesFor(shape, junk);
            for (int i = 0; i < seqs.size(); ++i) {
                final RowSequence seq = seqs.get(i);
                if (seq == null) {
                    continue;
                }
                final String name = NAMES.get(i);
                final List<Long> keys = new ArrayList<>();
                seq.forEachRowKey(k -> {
                    keys.add(k);
                    return keys.size() <= 6; // a correct walk yields exactly 6
                });
                assertEquals(name + ": keys", List.of(10L, 11L, 12L, MAX - 2, MAX - 1, MAX), keys);
                seq.close();
            }
        } finally {
            junk.forEach(SafeCloseableList::close);
        }
    }

}
