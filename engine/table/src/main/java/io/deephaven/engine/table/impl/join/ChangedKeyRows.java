//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;

/**
 * Splits the modified rows of an update into those whose key value changed and those whose key value did not, by
 * comparing the previous key values (read at the pre-shift row keys) with the current key values (read at the
 * post-shift row keys). A join only needs to move the rows whose key value changed between hash slots; the rest keep
 * their slot and cost no hash lookups.
 *
 * <p>
 * The previous key values of the changed rows are handed to an optional probe as each chunk is processed, so a hash
 * table keyed by the previous values can drop those rows without reading the previous values a second time.
 * </p>
 */
public class ChangedKeyRows {
    private final ChunkType[] chunkTypes;
    // per key-column equality kernels
    private final ChunkEquals[] keyChunkEquals;
    // per key-column compaction kernels, used to compact the previous key chunks down to the changed rows
    private final CompactKernel[] keyCompactKernels;

    public ChangedKeyRows(final ChunkType[] chunkTypes) {
        this.chunkTypes = chunkTypes;
        keyChunkEquals = new ChunkEquals[chunkTypes.length];
        keyCompactKernels = new CompactKernel[chunkTypes.length];
        for (int cc = 0; cc < chunkTypes.length; ++cc) {
            keyChunkEquals[cc] = ChunkEquals.makeEqual(chunkTypes[cc]);
            keyCompactKernels[cc] = CompactKernel.makeCompact(chunkTypes[cc]);
        }
    }

    /**
     * Find the modified rows whose key value changed.
     *
     * @param keySources the key sources, one per key column, read for both previous and current values
     * @param modifiedPreShift the modified rows in pre-shift key space, aligned positionally with
     *        {@code modifiedPostShift}
     * @param modifiedPostShift the modified rows in post-shift key space
     * @param changedPreShift output, ascending, receives the pre-shift keys of the rows whose key value changed,
     *        aligned with {@code changedPostShift}
     * @param changedPostShift output, ascending, receives the post-shift keys of the rows whose key value changed
     * @param previousKeyProbe if not null, called once per chunk with the changed rows' pre-shift row keys and their
     *        previous key values
     */
    public void findChanged(
            final ColumnSource<?>[] keySources,
            final RowSet modifiedPreShift,
            final RowSet modifiedPostShift,
            final RowSetBuilderSequential changedPreShift,
            final RowSetBuilderSequential changedPostShift,
            @Nullable final TypedHasherUtil.ProbeHandler previousKeyProbe) {
        if (modifiedPostShift.isEmpty()) {
            return;
        }
        final int numColumns = keySources.length;
        final int chunkSize = (int) Math.min(CHUNK_SIZE, modifiedPostShift.size());

        final ChunkSource.FillContext[] prevContexts = new ChunkSource.FillContext[numColumns];
        final ChunkSource.GetContext[] currentContexts = new ChunkSource.GetContext[numColumns];
        // noinspection unchecked
        final WritableChunk<Values>[] prevKeys = new WritableChunk[numColumns];

        try (
                final SafeCloseableArray<ChunkSource.FillContext> ignored = new SafeCloseableArray<>(prevContexts);
                final SafeCloseableArray<ChunkSource.GetContext> ignored2 = new SafeCloseableArray<>(currentContexts);
                final SafeCloseableArray<WritableChunk<Values>> ignored3 = new SafeCloseableArray<>(prevKeys);
                final WritableBooleanChunk<Any> comparisonResults = WritableBooleanChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedRowKeys> compactedPreRowKeys =
                        previousKeyProbe == null ? null : WritableLongChunk.makeWritableChunk(chunkSize);
                final SharedContext prevShared = SharedContext.makeSharedContext();
                final SharedContext currentShared = SharedContext.makeSharedContext();
                final RowSequence.Iterator preIt = modifiedPreShift.getRowSequenceIterator();
                final RowSequence.Iterator postIt = modifiedPostShift.getRowSequenceIterator()) {
            for (int cc = 0; cc < numColumns; ++cc) {
                prevContexts[cc] = keySources[cc].makeFillContext(chunkSize, prevShared);
                currentContexts[cc] = keySources[cc].makeGetContext(chunkSize, currentShared);
                prevKeys[cc] = chunkTypes[cc].makeWritableChunk(chunkSize);
            }

            while (postIt.hasMore()) {
                final RowSequence preChunkRows = preIt.getNextRowSequenceWithLength(chunkSize);
                final RowSequence postChunkRows = postIt.getNextRowSequenceWithLength(chunkSize);

                // comparisonResults holds true where previous and current are equal (the sense inverts below)
                final int chunkRsSize = postChunkRows.intSize();
                for (int cc = 0; cc < numColumns; ++cc) {
                    keySources[cc].fillPrevChunk(prevContexts[cc], prevKeys[cc], preChunkRows);
                    final Chunk<? extends Values> currentValues =
                            keySources[cc].getChunk(currentContexts[cc], postChunkRows);
                    if (cc == 0) {
                        keyChunkEquals[cc].equal(prevKeys[cc], currentValues, comparisonResults);
                    } else {
                        keyChunkEquals[cc].andEqual(prevKeys[cc], currentValues, comparisonResults);
                    }
                }

                final LongChunk<OrderedRowKeys> preKeys = preChunkRows.asRowKeyChunk();
                final LongChunk<OrderedRowKeys> postKeys = postChunkRows.asRowKeyChunk();
                int changedInChunk = 0;
                for (int ii = 0; ii < chunkRsSize; ++ii) {
                    final boolean changed = !comparisonResults.get(ii);
                    // comparisonResults now holds true where the key changed, which is what the compaction keeps
                    comparisonResults.set(ii, changed);
                    if (changed) {
                        changedPreShift.appendKey(preKeys.get(ii));
                        changedPostShift.appendKey(postKeys.get(ii));
                        if (compactedPreRowKeys != null) {
                            compactedPreRowKeys.set(changedInChunk, preKeys.get(ii));
                        }
                        changedInChunk++;
                    }
                }
                comparisonResults.setSize(chunkRsSize);

                if (previousKeyProbe != null && changedInChunk > 0) {
                    for (int cc = 0; cc < numColumns; ++cc) {
                        keyCompactKernels[cc].compact(prevKeys[cc], comparisonResults);
                    }
                    compactedPreRowKeys.setSize(changedInChunk);
                    try (final RowSequence changedRows =
                            RowSequenceFactory.wrapRowKeysChunkAsRowSequence(compactedPreRowKeys)) {
                        previousKeyProbe.doProbe(changedRows, prevKeys);
                    }
                }

                prevShared.reset();
                currentShared.reset();
            }
        }
    }
}
