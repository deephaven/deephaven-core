//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Per-subscriber {@link DictionaryWriterState} for full subscriptions (and growing subscriptions targeting a full
 * subscription). Delegates value-to-index lookups to a {@link SharedWriterDictionary} shared across all full
 * subscribers on the same table, and tracks an independent {@code flushedOffset} so each subscriber only receives
 * values it has not yet seen.
 *
 * <p>
 * On first use ({@link #needsFullBatch()} {@code == true}), {@link #buildDeltaChunk()} returns a chunk covering the
 * complete current value list so the subscriber receives an {@code isDelta=false} reset batch covering all values
 * accumulated before it joined. After {@link #resetDelta()} is called, only values added after that point are included
 * in future delta batches.
 *
 * <p>
 * Thread-safety: not thread-safe; access is serialized by the barrage propagation thread (the UGP cycle).
 */
public final class SharedDictionaryWriterState implements DictionaryWriterState {

    private final SharedWriterDictionary shared;
    /**
     * Index into {@link SharedWriterDictionary}'s value list up to which we have already sent to this subscriber.
     */
    private int flushedOffset = 0;
    /**
     * True when the next DictionaryBatch must be {@code isDelta=false} — on first use, or after a shared-dictionary
     * reset.
     */
    private boolean needsFullBatch = true;
    /**
     * The {@link SharedWriterDictionary#getGeneration()} value at the time this subscriber last synced with the shared
     * dictionary. The shared dictionary's generation starts at 0 and increments each time
     * {@link SharedWriterDictionary#reset()} is called (i.e. each time the shared dictionary is compacted because it
     * grew larger than the live row count).
     *
     * <p>
     * When a reset occurs, the shared value list is cleared and rebuilt from scratch, so all previously assigned
     * indices become invalid. Any client that cached those indices is now holding stale data. This field lets each
     * subscriber detect that staleness lazily: {@link #syncGeneration()} compares {@code lastSeenGeneration} against
     * the shared dictionary's current generation on every public method entry, and if they differ, resets
     * {@link #flushedOffset} to 0 and sets {@link #needsFullBatch} to {@code true} so the next DictionaryBatch is
     * emitted as {@code isDelta=false} (a full replacement). After syncing, {@code lastSeenGeneration} is updated to
     * match so subsequent calls within the same UGP cycle do not re-trigger the reset.
     *
     * <p>
     * Initialized from {@link SharedWriterDictionary#getGeneration()} at construction time so that a subscriber created
     * after one or more compactions does not incorrectly treat the first batch as a post-reset.
     */
    private int lastSeenGeneration;

    public SharedDictionaryWriterState(@NotNull final SharedWriterDictionary shared) {
        this.shared = shared;
        this.lastSeenGeneration = shared.getGeneration();
    }

    /**
     * Detects whether the shared dictionary has been reset since we last synced. If so, resets this subscriber's
     * position back to zero and marks it as needing a fresh {@code isDelta=false} batch.
     */
    private void syncGeneration() {
        final int currentGeneration = shared.getGeneration();
        if (lastSeenGeneration != currentGeneration) {
            flushedOffset = 0;
            needsFullBatch = true;
            lastSeenGeneration = currentGeneration;
        }
    }

    @Override
    public long getDictId() {
        return shared.getDictId();
    }

    @Override
    public void fillIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @NotNull final WritableIntChunk<Values> out) {
        shared.fillIndexChunk(source, subset, options, out);
    }

    @Override
    public boolean hasDelta() {
        syncGeneration();
        return needsFullBatch || flushedOffset < shared.getTotalSize();
    }

    @Override
    public boolean needsFullBatch() {
        syncGeneration();
        return needsFullBatch;
    }

    @Override
    @NotNull
    public WritableChunk<Values> buildDeltaChunk() {
        syncGeneration();
        return shared.buildDeltaChunk(flushedOffset, shared.getTotalSize());
    }

    @Override
    public void resetDelta() {
        syncGeneration();
        flushedOffset = shared.getTotalSize();
        needsFullBatch = false;
    }

    @Override
    public int totalSize() {
        return shared.getTotalSize();
    }

    /**
     * Not supported: the shared dictionary is compacted by calling {@link SharedWriterDictionary#reset()} directly;
     * this per-subscriber wrapper detects that reset lazily via {@link #syncGeneration()}. Calling reset() here is
     * always a caller error.
     */
    @Override
    public void reset() {
        throw new UnsupportedOperationException(
                "SharedDictionaryWriterState is reset via SharedWriterDictionary.reset()");
    }
}
