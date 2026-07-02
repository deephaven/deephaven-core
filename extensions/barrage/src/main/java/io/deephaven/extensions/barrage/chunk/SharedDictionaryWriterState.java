//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared per-table dictionary state for full subscriptions. Holds the authoritative value-to-index mapping and the
 * ordered list of all values ever added. Multiple {@link FullSubscriptionDictionaryState} instances (one per active
 * full subscriber) delegate their index lookups here, so all full subscribers observe the same index assignments.
 *
 * <p>
 * The value list grows monotonically and is never compacted here. Each per-subscriber wrapper tracks an independent
 * {@code flushedOffset} into this list so it knows which values have already been sent to that subscriber.
 *
 * <p>
 * Thread-safety: not thread-safe; access is serialized by the barrage propagation thread (the UGP cycle).
 */
public final class SharedDictionaryWriterState extends AbstractDictionaryWriterState {

    private final long dictId;
    /** All distinct values in insertion order, cleared on {@link #reset()}. */
    private final List<Object> allValues = new ArrayList<>();
    /**
     * Incremented each time {@link #reset()} is called. {@link FullSubscriptionDictionaryState} instances detect a
     * reset by comparing their stored generation against this value.
     */
    private int generation = 0;

    public SharedDictionaryWriterState(final long dictId, final ChunkType valuesChunkType) {
        super(valuesChunkType);
        this.dictId = dictId;
    }

    @Override
    protected int nextIndex() {
        return allValues.size();
    }

    @Override
    protected void recordNewValue(@NotNull final Object boxed, final int index) {
        allValues.add(boxed);
    }

    public long getDictId() {
        return dictId;
    }

    /** Returns an unmodifiable view of all values in insertion order. */
    @NotNull
    public List<Object> getAllValues() {
        return Collections.unmodifiableList(allValues);
    }

    /** Total number of distinct values currently in the dictionary (reset to 0 after {@link #reset()}). */
    public int getTotalSize() {
        return allValues.size();
    }

    /** Returns the current generation counter. Increments each time {@link #reset()} is called. */
    public int getGeneration() {
        return generation;
    }

    /**
     * Discards all accumulated values and increments the generation counter. {@link FullSubscriptionDictionaryState}
     * instances that reference this shared state will detect the reset on their next query and re-emit an
     * {@code isDelta=false} DictionaryBatch.
     */
    public void reset() {
        generation++;
        allValues.clear();
        clearMaps();
    }
}
