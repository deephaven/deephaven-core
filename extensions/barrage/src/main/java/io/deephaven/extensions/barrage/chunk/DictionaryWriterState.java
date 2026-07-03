//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Tracks the cumulative dictionary for one Arrow dictionary id within a barrage stream. Shared by all
 * {@link DictionaryChunkWriter} instances that reference the same id.
 *
 * <p>
 * {@link #indexForObject(Object)} is called once per logical row (non-null) while building a batch. After the batch's
 * {@link org.apache.arrow.flatbuf.DictionaryBatch} has been emitted, call {@link #resetDelta()} to advance the delta
 * boundary.
 *
 * <p>
 * Two concrete implementations exist:
 * <ul>
 * <li>{@link LocalDictionaryWriterState} — for viewport subscriptions and snapshots. {@code resetDelta()} clears the
 * delta list entirely; only newly-seen values since the last reset are tracked.</li>
 * <li>{@link FullSubscriptionDictionaryState} — for full subscriptions and growing subscriptions. The full cumulative
 * value list is retained so that a new subscriber joining mid-stream can receive all current values as an initial
 * {@code isDelta=false} batch.</li>
 * </ul>
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded barrage stream serialization is assumed.
 */
public interface DictionaryWriterState {

    long getDictId();

    int indexForObject(@NotNull Object value);

    int indexForByte(byte v);

    int indexForChar(char v);

    int indexForShort(short v);

    int indexForInt(int v);

    int indexForLong(long v);

    int indexForFloat(float v);

    int indexForDouble(double v);

    /**
     * Returns {@code true} if a DictionaryBatch message needs to be emitted before the current RecordBatch — either
     * because this is the first batch for this subscriber ({@code isDelta=false}) or because new values have been added
     * since the last reset.
     */
    boolean hasDelta();

    boolean needsFullBatch();

    /**
     * Returns the ordered list of values that form the current delta (values added since the last {@link #resetDelta}
     * call, or all values if this is the first batch for this subscriber).
     */
    @NotNull
    List<Object> getDeltaValues();

    /**
     * Advances the delta boundary after a DictionaryBatch has been successfully emitted. Unlike {@link #reset()}, this
     * does not discard the accumulated value-to-index mapping — it only moves the boundary so that already-sent values
     * are excluded from future delta batches. The client's cached dictionary remains valid after this call.
     */
    void resetDelta();

    /** Current number of distinct values in the dictionary (resets to zero after {@link #reset()}). */
    int totalSize();

    /**
     * Resets the dictionary to an empty state, as if no values had ever been seen. The next DictionaryBatch emitted
     * will be {@code isDelta=false} with only the values encountered in the next batch. Call this when the cumulative
     * dictionary size exceeds the live row count and compaction is needed.
     */
    void reset();
}
