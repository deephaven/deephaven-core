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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Shared per-table dictionary state for full subscriptions. Holds the authoritative value-to-index mapping and the
 * ordered list of all values ever added. Multiple {@link SharedDictionaryWriterState} instances (one per active full
 * subscriber) delegate their index lookups here, so all full subscribers observe the same index assignments.
 *
 * <p>
 * The value list grows monotonically and is never compacted here. Each per-subscriber wrapper tracks an independent
 * {@code flushedOffset} into this list so it knows which values have already been sent to that subscriber.
 *
 * <p>
 * Thread-safety: not thread-safe; access is serialized by the barrage propagation thread (the UGP cycle).
 */
public final class SharedWriterDictionary {

    private final long dictId;
    private final DictionaryWriterValueMap map;
    /**
     * Incremented each time {@link #reset()} is called. {@link SharedDictionaryWriterState} instances detect a reset by
     * comparing their stored generation against this value.
     */
    private int generation = 0;

    public SharedWriterDictionary(final long dictId, final ChunkType valuesChunkType) {
        this.dictId = dictId;
        this.map = DictionaryWriterValueMap.make(valuesChunkType);
    }

    public long getDictId() {
        return dictId;
    }

    void fillIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @NotNull final WritableIntChunk<Values> out) {
        map.fillIndexChunk(source, subset, options.useDeephavenNulls(), out);
    }

    /** Total number of distinct values currently in the dictionary (reset to 0 after {@link #reset()}). */
    public int getTotalSize() {
        return map.size();
    }

    /**
     * Builds and returns a typed chunk containing the values in {@code [fromOffset, toOffset)}. The returned chunk is
     * owned by the caller and must be closed when no longer needed.
     */
    @NotNull
    WritableChunk<Values> buildDeltaChunk(final int fromOffset, final int toOffset) {
        return map.buildChunk(fromOffset, toOffset);
    }

    /** Returns the current generation counter. Increments each time {@link #reset()} is called. */
    public int getGeneration() {
        return generation;
    }

    /**
     * Discards all accumulated values and increments the generation counter. {@link SharedDictionaryWriterState}
     * instances that reference this shared dictionary will detect the reset on their next query and re-emit an
     * {@code isDelta=false} DictionaryBatch.
     */
    public void reset() {
        generation++;
        map.reset();
    }
}
