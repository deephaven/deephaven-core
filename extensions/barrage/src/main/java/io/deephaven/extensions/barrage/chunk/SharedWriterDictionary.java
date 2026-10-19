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
 * Shared per-table dictionary state for full subscriptions. Holds the authoritative value-to-index mapping and the
 * ordered list of all values ever added. Multiple {@link SharedDictionaryWriterState} instances (one per active full
 * subscriber) delegate their index lookups here, so all full subscribers observe the same index assignments.
 *
 * <p>
 * The value list only grows, until {@link #reset()} discards it. Each per-subscriber wrapper tracks an independent
 * {@code flushedOffset} into this list so it knows which values have already been sent to that subscriber.
 *
 * <p>
 * Thread-safety: the producer writes to its subscribers in parallel, so full subscribers fill, measure and read this
 * dictionary from different threads at once. Filling and copying hold the dictionary's monitor, once per chunk filled
 * and once per batch copied, since a fill may grow the value list while a copy reads it; the values are read only under
 * the monitor. Measuring does not hold it: the size is a volatile field, so {@link #getTotalSize()} gives the
 * subscribers' {@code hasDelta()} and {@code totalSize()} checks an up-to-date count without blocking, and because
 * values are only appended, a range of the list that a subscriber has measured stays valid however many values are
 * added afterwards. {@link #reset()} must be called only while no subscriber is writing.
 */
public final class SharedWriterDictionary {

    private final long dictId;
    private final DictionaryWriterValueMap map;
    /**
     * Incremented each time {@link #reset()} is called. {@link SharedDictionaryWriterState} instances detect a reset by
     * comparing their stored generation against this value.
     */
    private volatile int generation = 0;
    /** The number of values in {@link #map}, published after the values it counts. */
    private volatile int size = 0;

    public SharedWriterDictionary(final long dictId, final ChunkType valuesChunkType) {
        this.dictId = dictId;
        this.map = DictionaryWriterValueMap.make(valuesChunkType);
    }

    public long getDictId() {
        return dictId;
    }

    /** Fills {@code out} with the index of each value, registering values as it meets them. */
    synchronized void fillIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @NotNull final WritableIntChunk<Values> out) {
        try {
            map.fillIndexChunk(source, subset, options.useDeephavenNulls(), out);
        } finally {
            // a fill that failed part way may still have added values; count them too
            size = map.size();
        }
    }

    /** Total number of distinct values currently in the dictionary (reset to 0 after {@link #reset()}). */
    public int getTotalSize() {
        return size;
    }

    /**
     * Builds and returns a typed chunk containing the values in {@code [fromOffset, toOffset)}. The returned chunk is
     * owned by the caller and must be closed when no longer needed.
     */
    @NotNull
    synchronized WritableChunk<Values> buildDeltaChunk(final int fromOffset, final int toOffset) {
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
    public synchronized void reset() {
        map.reset();
        size = 0;
        generation++;
    }
}
