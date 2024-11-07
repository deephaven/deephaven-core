//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.List;

public class SnapshotTestUtils {

    private static final double DELTA = 1e-5;

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
        for (int colId = 0; colId < numColumns; colId++) {
            final ColumnSource<?> columnSource = expected.getColumnSource(columnNames.get(colId));
            try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(expected.intSize());
                    final WritableChunk<Values> writableChunk =
                            columnSource.getChunkType().makeWritableChunk(numRows)) {
                columnSource.fillChunk(fillContext, writableChunk, expected.getRowSet());
                final List<Chunk<Values>> snapshotColumn = snap.addColumnData[colId].data;
                for (int rowId = 0; rowId < numRows; rowId++) {
                    if (writableChunk instanceof WritableByteChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableByteChunk().get(rowId),
                                ((ByteChunk<?>) snapshotColumn.get(0)).get(rowId));
                    } else if (writableChunk instanceof WritableShortChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableShortChunk().get(rowId),
                                ((ShortChunk<?>) snapshotColumn.get(0)).get(rowId));
                    } else if (writableChunk instanceof WritableIntChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableIntChunk().get(rowId),
                                ((IntChunk<?>) snapshotColumn.get(0)).get(rowId));
                    } else if (writableChunk instanceof WritableLongChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableLongChunk().get(rowId),
                                ((LongChunk<?>) snapshotColumn.get(0)).get(rowId));
                    } else if (writableChunk instanceof WritableFloatChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableFloatChunk().get(rowId),
                                ((FloatChunk<?>) snapshotColumn.get(0)).get(rowId),
                                DELTA);
                    } else if (writableChunk instanceof WritableDoubleChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableDoubleChunk().get(rowId),
                                ((DoubleChunk<?>) snapshotColumn.get(0)).get(rowId),
                                DELTA);
                    } else if (writableChunk instanceof WritableCharChunk) {
                        Assert.assertEquals(
                                writableChunk.asWritableCharChunk().get(rowId),
                                ((CharChunk<?>) snapshotColumn.get(0)).get(rowId));
                    } else if (writableChunk instanceof WritableObjectChunk &&
                            snapshotColumn.get(0) instanceof WritableByteChunk) {
                        Assert.assertEquals(
                                BooleanUtils.booleanAsByte((Boolean) writableChunk.asWritableObjectChunk().get(rowId)),
                                ((ByteChunk<?>) snapshotColumn.get(0)).get(rowId));
                    } else {
                        Assert.assertEquals(
                                writableChunk.asWritableObjectChunk().get(rowId),
                                ((ObjectChunk<?, ?>) snapshotColumn.get(0)).get(rowId));
                    }
                }
            }
        }
    }
}
