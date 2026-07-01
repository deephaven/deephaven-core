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
 * {@link DictionaryWriterState} implementation for viewport subscriptions and snapshots. {@link #resetDelta()} clears
 * the delta list entirely; only newly-seen values since the last reset are retained.
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded barrage stream serialization is assumed.
 */
public final class LocalDictionaryWriterState implements DictionaryWriterState {

    private final long dictId;
    /** Maps each known value to its 0-based dictionary index. Cleared on {@link #reset()}. */
    private final Object2IntMap<Object> valueToIndex = new Object2IntOpenHashMap<>();
    /** Values added since the last {@link #resetDelta()}; also cleared on {@link #reset()}. */
    private final List<Object> deltaValues = new ArrayList<>();
    /** Total number of values ever added (= global dictionary size; used to assign the next index). */
    private int totalSize = 0;
    /**
     * True when the next DictionaryBatch must be {@code isDelta=false} — on first use, or after a local dictionary
     * reset.
     */
    private boolean needsFullBatch = true;

    public LocalDictionaryWriterState(final long dictId) {
        this.dictId = dictId;
        valueToIndex.defaultReturnValue(-1);
    }

    @Override
    public long getDictId() {
        return dictId;
    }

    @Override
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

    @Override
    public boolean hasDelta() {
        return needsFullBatch || !deltaValues.isEmpty();
    }

    @Override
    public boolean needsFullBatch() {
        return needsFullBatch;
    }

    @Override
    @NotNull
    public List<Object> getDeltaValues() {
        return deltaValues;
    }

    @Override
    public void resetDelta() {
        deltaValues.clear();
        needsFullBatch = false;
    }

    @Override
    public int totalSize() {
        return totalSize;
    }

    @Override
    public void reset() {
        valueToIndex.clear();
        deltaValues.clear();
        totalSize = 0;
        needsFullBatch = true;
    }
}
