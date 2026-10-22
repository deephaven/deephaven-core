//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InverseWrappedRowSetRowRedirectionTest extends RefreshingTableTestCase {

    /**
     * @return every eighth key below 8000, followed by a run of 64 consecutive keys
     */
    private static TrackingWritableRowSet makeRowSet() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long key = 0; key < 8000; key += 8) {
            builder.appendKey(key);
        }
        builder.appendRange(10_000, 10_063);
        return builder.build().toTracking();
    }

    @Test
    public void testFillChunk() {
        final TrackingWritableRowSet rowSet = makeRowSet();
        final InverseWrappedRowSetRowRedirection redirection = new InverseWrappedRowSetRowRedirection(rowSet);

        // contiguous slices of the row set, including the whole row set and a single key
        assertMapping(redirection, rowSet, rowSet, false);
        try (final RowSet slice = rowSet.subSetByPositionRange(100, 900);
                final RowSet singleKey = rowSet.subSetByPositionRange(37, 38);
                final RowSet acrossRun = rowSet.subSetByPositionRange(990, 1030)) {
            assertMapping(redirection, slice, rowSet, false);
            assertMapping(redirection, singleKey, rowSet, false);
            assertMapping(redirection, acrossRun, rowSet, false);
        }

        // subsets that skip keys of the row set
        final RowSetBuilderSequential everyOtherBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential endsOnlyBuilder = RowSetFactory.builderSequential();
        endsOnlyBuilder.appendKey(rowSet.firstRowKey());
        endsOnlyBuilder.appendKey(rowSet.lastRowKey());
        final MutableLong position = new MutableLong(0);
        rowSet.forAllRowKeys(key -> {
            if (position.getAndIncrement() % 2 == 0) {
                everyOtherBuilder.appendKey(key);
            }
        });
        try (final RowSet everyOther = everyOtherBuilder.build();
                final RowSet endsOnly = endsOnlyBuilder.build()) {
            assertMapping(redirection, everyOther, rowSet, false);
            assertMapping(redirection, endsOnly, rowSet, false);
        }

        try (final RowSet empty = RowSetFactory.empty()) {
            assertMapping(redirection, empty, rowSet, false);
        }
    }

    @Test
    public void testFillPrevChunk() {
        final TrackingWritableRowSet rowSet = makeRowSet();
        final InverseWrappedRowSetRowRedirection redirection = new InverseWrappedRowSetRowRedirection(rowSet);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            // shift the positions of every key after the first by inserting a key, and append more keys
            rowSet.insert(1);
            rowSet.insertRange(20_000, 20_099);

            try (final RowSet prevRowSet = rowSet.copyPrev();
                    final RowSet prevSlice = prevRowSet.subSetByPositionRange(100, 900);
                    final RowSet added = RowSetFactory.fromRange(20_000, 20_099)) {
                assertMapping(redirection, prevRowSet, prevRowSet, true);
                assertMapping(redirection, prevSlice, prevRowSet, true);
                assertMapping(redirection, prevSlice, rowSet, false);
                assertMapping(redirection, added, rowSet, false);
            }
        });
    }

    private static void assertMapping(
            final InverseWrappedRowSetRowRedirection redirection,
            final RowSet keysToMap,
            final RowSet expectedRowSet,
            final boolean usePrev) {
        try (final WritableLongChunk<RowKeys> mappedKeys = WritableLongChunk.makeWritableChunk(keysToMap.intSize())) {
            if (usePrev) {
                redirection.fillPrevChunk(null, mappedKeys, keysToMap);
            } else {
                redirection.fillChunk(null, mappedKeys, keysToMap);
            }
            assertEquals(keysToMap.intSize(), mappedKeys.size());
            final MutableInt chunkPosition = new MutableInt(0);
            keysToMap.forAllRowKeys(key -> {
                assertEquals("position of " + key, expectedRowSet.find(key),
                        mappedKeys.get(chunkPosition.getAndIncrement()));
            });
        }
    }
}
