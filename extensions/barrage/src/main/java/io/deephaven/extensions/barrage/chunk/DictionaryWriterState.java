//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks the cumulative dictionary for one Arrow dictionary id within a single barrage stream (snapshot or subscription
 * update sequence). Shared by all {@link DictionaryChunkWriter} instances that reference the same id.
 *
 * <p>
 * {@link #indexFor(Object)} is called once per logical row (non-null) while building a batch. Values added since the
 * last reset are accumulated in insertion order as the delta for the current batch. After the batch's
 * {@link org.apache.arrow.flatbuf.DictionaryBatch} has been emitted, call {@link #resetDelta()} to clear the delta.
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded barrage stream serialization is assumed.
 */
public final class DictionaryWriterState {

    private final long dictId;
    /** Maps every value ever seen to its 0-based dictionary index. Never cleared. */
    private final Object2IntMap<Object> valueToIndex = new Object2IntOpenHashMap<>();
    /** Values added since the last {@link #resetDelta}; cleared on each reset. */
    private final List<Object> deltaValues = new ArrayList<>();
    /** Total number of values ever added (= global dictionary size; used to assign the next index). */
    private int totalSize = 0;
    /** True until the first DictionaryBatch has been emitted for this stream. */
    private boolean firstBatch = true;

    public DictionaryWriterState(final long dictId) {
        this.dictId = dictId;
        valueToIndex.defaultReturnValue(-1);
    }

    public long getDictId() {
        return dictId;
    }

    /**
     * Returns the 0-based dictionary index for {@code value}, adding it to the dictionary if not already present. Must
     * not be called with {@code null}; null rows should be handled by the caller (null index, validity bit = 0).
     */
    public int indexFor(@NotNull final Object value) {
        final int existing = valueToIndex.getInt(value);
        if (existing != -1) {
            return existing;
        }

        final int index = totalSize++;
        deltaValues.add(value);
        valueToIndex.put(value, index);
        return index;
    }

    /**
     * Returns {@code true} if a DictionaryBatch message needs to be emitted before the current RecordBatch — either
     * because this is the first batch for the stream ({@code isDelta=false}) or because new values have been added
     * since the last reset.
     */
    public boolean hasDelta() {
        return firstBatch || !deltaValues.isEmpty();
    }

    public boolean isFirstBatch() {
        return firstBatch;
    }

    /**
     * Returns the ordered list of values that form the current delta (values added since the last {@link #resetDelta}
     * call, or all values if this is the first batch).
     */
    @NotNull
    public List<Object> getDeltaValues() {
        return deltaValues;
    }

    /**
     * Clears the delta and advances the first-batch flag. Call this after successfully emitting the DictionaryBatch for
     * the current batch.
     */
    public void resetDelta() {
        deltaValues.clear();
        firstBatch = false;
    }
}
