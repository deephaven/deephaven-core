//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Per-subscriber {@link DictionaryWriterState} for full subscriptions (and growing subscriptions targeting a full
 * subscription). Delegates value-to-index lookups to a {@link SharedDictionaryWriterState} shared across all full
 * subscribers on the same table, and tracks an independent {@code flushedOffset} so each subscriber only receives
 * values it has not yet seen.
 *
 * <p>
 * On first use ({@link #needsFullBatch()} {@code == true}), {@link #getDeltaValues()} returns the complete current
 * value list so the subscriber receives an {@code isDelta=false} reset batch covering all values accumulated before it
 * joined. After {@link #resetDelta()} is called, only values added after that point are included in future delta
 * batches.
 *
 * <p>
 * Thread-safety: not thread-safe; access is serialized by the barrage propagation thread (the UGP cycle).
 */
public final class FullSubscriptionDictionaryWriterState implements DictionaryWriterState {

    private final SharedDictionaryWriterState shared;
    /**
     * Index into {@link SharedDictionaryWriterState#getAllValues()} up to which we have already sent to this
     * subscriber.
     */
    private int flushedOffset = 0;
    /**
     * True when the next DictionaryBatch must be {@code isDelta=false} — on first use, or after a shared-state reset.
     */
    private boolean needsFullBatch = true;
    /**
     * The {@link SharedDictionaryWriterState#getGeneration()} value at the time this subscriber last synced with the
     * shared state. The shared state's generation starts at 0 and increments each time
     * {@link SharedDictionaryWriterState#reset()} is called (i.e. each time the shared dictionary is compacted because
     * it grew larger than the live row count).
     *
     * <p>
     * When a reset occurs, the shared value list is cleared and rebuilt from scratch, so all previously assigned
     * indices become invalid. Any client that cached those indices is now holding stale data. This field lets each
     * subscriber detect that staleness lazily: {@link #syncGeneration()} compares {@code lastSeenGeneration} against
     * the shared state's current generation on every public method entry, and if they differ, resets
     * {@link #flushedOffset} to 0 and sets {@link #needsFullBatch} to {@code true} so the next DictionaryBatch is
     * emitted as {@code isDelta=false} (a full replacement). After syncing, {@code lastSeenGeneration} is updated to
     * match so subsequent calls within the same UGP cycle do not re-trigger the reset.
     *
     * <p>
     * Initialized from {@link SharedDictionaryWriterState#getGeneration()} at construction time so that a subscriber
     * created after one or more compactions does not incorrectly treat the first batch as a post-reset.
     */
    private int lastSeenGeneration;

    public FullSubscriptionDictionaryWriterState(@NotNull final SharedDictionaryWriterState shared) {
        this.shared = shared;
        this.lastSeenGeneration = shared.getGeneration();
    }

    /**
     * Detects whether the shared state has been reset since we last synced. If so, resets this subscriber's position
     * back to zero and marks it as needing a fresh {@code isDelta=false} batch.
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
    public int indexForObject(@NotNull final Object value) {
        return shared.indexForObject(value);
    }

    @Override
    public int indexForByte(final byte v) {
        return shared.indexForByte(v);
    }

    @Override
    public int indexForChar(final char v) {
        return shared.indexForChar(v);
    }

    @Override
    public int indexForShort(final short v) {
        return shared.indexForShort(v);
    }

    @Override
    public int indexForInt(final int v) {
        return shared.indexForInt(v);
    }

    @Override
    public int indexForLong(final long v) {
        return shared.indexForLong(v);
    }

    @Override
    public int indexForFloat(final float v) {
        return shared.indexForFloat(v);
    }

    @Override
    public int indexForDouble(final double v) {
        return shared.indexForDouble(v);
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
    public List<Object> getDeltaValues() {
        syncGeneration();
        return shared.getAllValues().subList(flushedOffset, shared.getTotalSize());
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
     * Not supported: the shared dictionary is compacted by calling {@link SharedDictionaryWriterState#reset()}
     * directly; this per-subscriber wrapper detects that reset lazily via {@link #syncGeneration()}. Calling reset()
     * here is always a caller error.
     */
    @Override
    public void reset() {
        throw new UnsupportedOperationException(
                "FullSubscriptionDictionaryWriterState is reset via SharedDictionaryWriterState.reset()");
    }
}
