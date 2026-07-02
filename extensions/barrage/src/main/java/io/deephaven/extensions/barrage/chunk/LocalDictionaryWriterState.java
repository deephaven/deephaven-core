//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
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
public final class LocalDictionaryWriterState extends AbstractDictionaryWriterState
        implements DictionaryWriterState {

    private final long dictId;
    /** Values added since the last {@link #resetDelta()}; also cleared on {@link #reset()}. */
    private final List<Object> deltaValues = new ArrayList<>();
    /** Total number of values ever added (= global dictionary size; used to assign the next index). */
    private int totalSize = 0;
    /**
     * True when the next DictionaryBatch must be {@code isDelta=false} — on first use, or after a local dictionary
     * reset.
     */
    private boolean needsFullBatch = true;

    public LocalDictionaryWriterState(final long dictId, final ChunkType valuesChunkType) {
        super(valuesChunkType);
        this.dictId = dictId;
    }

    @Override
    protected int nextIndex() {
        return totalSize++;
    }

    @Override
    protected void recordNewValue(@NotNull final Object boxed, final int index) {
        deltaValues.add(boxed);
    }

    @Override
    public long getDictId() {
        return dictId;
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
        clearMaps();
        deltaValues.clear();
        totalSize = 0;
        needsFullBatch = true;
    }
}
