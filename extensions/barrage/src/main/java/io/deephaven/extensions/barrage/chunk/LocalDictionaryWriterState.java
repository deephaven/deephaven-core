//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.chunk.writermap.DictionaryWriterValueMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DictionaryWriterState} implementation for viewport subscriptions and snapshots. A single
 * {@link DictionaryWriterValueMap} holds all distinct values in insertion order; {@code deltaStartOffset} marks the
 * boundary between values already flushed to the subscriber and values that still need to be sent.
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded barrage stream serialization is assumed.
 */
public final class LocalDictionaryWriterState implements DictionaryWriterState {

    private final long dictId;
    private final DictionaryWriterValueMap map;
    /** Values at offsets below this have already been sent; {@code [deltaStartOffset, map.size())} is the delta. */
    private int deltaStartOffset = 0;
    /**
     * True when the next DictionaryBatch must be {@code isDelta=false} — on first use, or after a local dictionary
     * reset.
     */
    private boolean needsFullBatch = true;

    public LocalDictionaryWriterState(final long dictId, final ChunkType valuesChunkType) {
        this.dictId = dictId;
        this.map = DictionaryWriterValueMap.make(valuesChunkType);
    }

    @Override
    public long getDictId() {
        return dictId;
    }

    @Override
    public void fillIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @NotNull final WritableIntChunk<Values> out) {
        map.fillIndexChunk(source, subset, options.useDeephavenNulls(), out);
    }

    @Override
    public boolean hasDelta() {
        return needsFullBatch || deltaStartOffset < map.size();
    }

    @Override
    public boolean needsFullBatch() {
        return needsFullBatch;
    }

    @Override
    @NotNull
    public WritableChunk<Values> buildDeltaChunk() {
        return map.buildChunk(deltaStartOffset, map.size());
    }

    @Override
    public void resetDelta() {
        deltaStartOffset = map.size();
        needsFullBatch = false;
    }

    @Override
    public int totalSize() {
        return map.size();
    }

    @Override
    public void reset() {
        map.reset();
        deltaStartOffset = 0;
        needsFullBatch = true;
    }
}
