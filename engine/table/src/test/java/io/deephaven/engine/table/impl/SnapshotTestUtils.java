//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import java.util.Iterator;
import java.util.List;

public class SnapshotTestUtils {

    /**
     * Used to compare snapshot data inside a {@link BarrageMessage} generated using
     * {@link ConstructSnapshot#constructBackplaneSnapshot(Object, BaseTable)} with the expected data in a
     * {@link Table}.
     */
    public static void verifySnapshotBarrageMessage(@NotNull final BarrageMessage snap, @NotNull final Table expected) {
        Assert.assertEquals(snap.rowsAdded.size(), expected.size());
        Assert.assertEquals(snap.addColumnData.length, expected.getColumnSources().size());

        final int numColumns = expected.getColumnSources().size();
        final int numRows = expected.intSize();
        final List<String> columnNames = expected.getDefinition().getColumnNames();
        final int maxSliceSize = Math.min(ArrayBackedColumnSource.BLOCK_SIZE, numRows);

        for (int colId = 0; colId < numColumns; colId++) {
            final ChunkSource<Values> expectedSource = ReinterpretUtils.maybeConvertToPrimitive(
                    expected.getColumnSource(columnNames.get(colId)));
            final ChunkType chunkType = expectedSource.getChunkType();

            // @formatter:off
            try (final ChunkSource.GetContext expectedGetContext = expectedSource.makeGetContext(maxSliceSize);
                 final RowSequence.Iterator expectedRows = expected.getRowSet().getRowSequenceIterator();
                 final ResettableReadOnlyChunk<Values> snapshotSlice = chunkType.makeResettableReadOnlyChunk()) {
                // @formatter:on

                final Iterator<Chunk<Values>> snapshotChunks = snap.addColumnData[colId].data.iterator();
                final ChunkEquals chunkEquals = ChunkEquals.makeEqual(chunkType);

                while (snapshotChunks.hasNext()) {
                    Assert.assertTrue(expectedRows.hasMore());
                    final Chunk<Values> snapshotChunk = snapshotChunks.next();
                    final int snapshotChunkSize = snapshotChunk.size();
                    for (int snapshotChunkUsed = 0; snapshotChunkUsed < snapshotChunkSize;) {
                        final int sliceSize = Math.min(snapshotChunkSize - snapshotChunkUsed, maxSliceSize);
                        final Chunk<? extends Values> expectedSlice = expectedSource.getChunk(expectedGetContext,
                                expectedRows.getNextRowSequenceWithLength(sliceSize));
                        snapshotSlice.resetFromChunk(snapshotChunk, snapshotChunkUsed, sliceSize);
                        Assert.assertTrue(chunkEquals.equalReduce(expectedSlice, snapshotSlice));
                        snapshotChunkUsed += sliceSize;
                    }
                }
                Assert.assertFalse(expectedRows.hasMore());
            }
        }
    }
}
