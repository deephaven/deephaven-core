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
 * Tracks the cumulative dictionary for one Arrow dictionary id within a barrage stream. Shared by all
 * {@link DictionaryChunkWriter} instances that reference the same id.
 *
 * <p>
 * {@link #fillIndexChunk} is called once per batch (non-null rows only) while building a batch. After the batch's
 * {@link org.apache.arrow.flatbuf.DictionaryBatch} has been emitted, call {@link #resetDelta()} to advance the delta
 * boundary.
 *
 * <p>
 * Two concrete implementations exist:
 * <ul>
 * <li>{@link LocalDictionaryWriterState} — for viewport subscriptions and snapshots. {@code resetDelta()} clears the
 * delta list entirely; only newly-seen values since the last reset are tracked.</li>
 * <li>{@link SharedDictionaryWriterState} — for full subscriptions and growing subscriptions. The full cumulative value
 * list is retained so that a new subscriber joining mid-stream can receive all current values as an initial
 * {@code isDelta=false} batch.</li>
 * </ul>
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded barrage stream serialization is assumed.
 */
public interface DictionaryWriterState {

    long getDictId();

    /**
     * Fills {@code out} with one dictionary index per logical row in {@code source}/{@code subset}. Null rows (in
     * non-deephaven-nulls mode) produce {@code QueryConstants.NULL_INT}; non-null rows produce a non-negative
     * dictionary index, registering new values as needed.
     *
     * @param source the source chunk containing column values
     * @param subset row positions within {@code source} to include; {@code null} means all rows
     * @param options barrage serialization options (e.g. {@code useDeephavenNulls})
     * @param out pre-sized output chunk to fill with dictionary indices
     */
    void fillIndexChunk(
            @NotNull Chunk<Values> source,
            @Nullable RowSet subset,
            @NotNull BarrageOptions options,
            @NotNull WritableIntChunk<Values> out);

    /**
     * Returns {@code true} if a DictionaryBatch message needs to be emitted before the current RecordBatch — either
     * because this is the first batch for this subscriber ({@code isDelta=false}) or because new values have been added
     * since the last reset.
     */
    boolean hasDelta();

    boolean needsFullBatch();

    /**
     * Builds and returns a typed chunk containing the current delta values (values added since the last
     * {@link #resetDelta} call, or all values if this is the first batch for this subscriber). The returned chunk is
     * owned by the caller and must be closed when no longer needed.
     */
    @NotNull
    WritableChunk<Values> buildDeltaChunk();

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
