/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import gnu.trove.map.TLongLongMap;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * Writable {@link RowRedirection}.
 *
 * A WritableRowRedirection can be in one of two states: tracking prev values or not. The typical lifecycle looks like
 * this:
 * <ol>
 * <li>A WritableRowRedirection is created with an initial map, but not tracking prev values. In this state, get() and
 * getPrev() behave identically; put() and remove() affect current values but do no "prev value" tracking.
 * <li>Prev value tracking begins when the caller calls startTrackingPrevValues(). Immediately after this call, the data
 * is logically "forked": getPrev() will still refer to the same set of entries as before; this set will be frozen until
 * the end of the generation.
 * <li>Additionally, a terminal listener will be registered so that the prev map will be updated at the end of the
 * generation.
 * <li>Meanwhile, get(), put(), and remove() will logically refer to a fork of that map: it will initially have the same
 * entries as prev, but it will diverge over time as the caller does put() and remove() operations.
 * <li>At the end of the generation (when the TerminalListener runs), the prev set is (logically) discarded, prev gets
 * current, and current becomes the new fork of the map.
 * </ol>
 */
public interface WritableRowRedirection extends RowRedirection, ChunkSink<RowKeys> {

    /**
     * Initiate previous value tracking.
     */
    void startTrackingPrevValues();

    /**
     * Add or change a mapping from {@code outerRowKey} to {@code innerRowKey}.
     *
     * @param outerRowKey The outer row key to map from
     * @param innerRowKey The inner row key to map to
     * @return The inner row key previously mapped from {@code outerRowKey}, or {@link RowSet#NULL_ROW_KEY} if there was
     *         no mapping
     */
    long put(long outerRowKey, long innerRowKey);

    /**
     * Remove a mapping from {@code outerRowKey}.
     *
     * @param outerRowKey The outer row key to unmap
     * @return The inner row key previously mapped from {@code outerRowKey}, or {@link RowSet#NULL_ROW_KEY} if there was
     *         no mapping
     */
    long remove(long outerRowKey);

    /**
     * Like {@link #put(long, long)}, but without requiring the implementation to provide a return value. May be more
     * efficient in some cases.
     *
     * @param outerRowKey The outer row key to map from
     * @param innerRowKey The inner row key to map to
     */
    default void putVoid(final long outerRowKey, final long innerRowKey) {
        put(outerRowKey, innerRowKey);
    }

    /**
     * Like {@link #remove(long)} (long, long)}, but without requiring the implementation to provide a return value. May
     * be more efficient in some cases.
     *
     * @param outerRowKey The outer row key to map from
     */
    default void removeVoid(final long outerRowKey) {
        remove(outerRowKey);
    }

    /**
     * Remove the specified {@code outerRowKeys}.
     *
     * @param outerRowKeys The outer row keys to remove
     */
    default void removeAll(final RowSequence outerRowKeys) {
        outerRowKeys.forAllRowKeys(this::remove);
    }

    /**
     * Insert mappings from each element in a {@link RowSequence} to the parallel element in a {@link LongChunk}. h
     * 
     * @param fillFromContext THe FillFromContext
     * @param innerRowKeys The inner row keys to map to
     * @param outerRowKeys The outer row keys to map from
     */
    @Override
    default void fillFromChunk(
            @NotNull final ChunkSink.FillFromContext fillFromContext,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final MutableInt offset = new MutableInt();
        final LongChunk<? extends RowKeys> innerRowKeysTyped = innerRowKeys.asLongChunk();
        outerRowKeys.forAllRowKeys(outerRowKey -> {
            final long innerRowKey = innerRowKeysTyped.get(offset.intValue());
            if (innerRowKey == RowSequence.NULL_ROW_KEY) {
                removeVoid(outerRowKey);
            } else {
                putVoid(outerRowKey, innerRowKey);
            }
            offset.increment();
        });
    }

    @Override
    default void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final LongChunk<RowKeys> outerRowKeys) {
        final LongChunk<? extends RowKeys> innerRowKeysTyped = innerRowKeys.asLongChunk();
        final int size = innerRowKeysTyped.size();
        for (int ki = 0; ki < size; ++ki) {
            final long outerRowKey = outerRowKeys.get(ki);
            final long innerRowKey = innerRowKeysTyped.get(ki);
            if (innerRowKey == RowSequence.NULL_ROW_KEY) {
                removeVoid(outerRowKey);
            } else {
                putVoid(outerRowKey, innerRowKey);
            }
        }
    }

    /**
     * Update this WritableRowRedirection according to a {@link RowSetShiftData}.
     *
     * @param tableRowSet A {@link RowSet} to filter which rows should be shifted
     * @param shiftData The {@link RowSetShiftData} for this update
     */
    default void applyShift(final RowSet tableRowSet, final RowSetShiftData shiftData) {
        RowRedirectionUtils.applyRedirectionShift(this, tableRowSet, shiftData);
    }

    /**
     * Factory for producing WritableRowSets and their components.
     */
    interface Factory {
        TLongLongMap createUnderlyingMapWithCapacity(int initialCapacity);

        WritableRowRedirection createRowRedirection(int initialCapacity);

        /**
         * @param map The initial {@link TLongLongMap} to use for backing the result WritableRowRedirection. Needs to
         *        have the same dynamic type as that returned by {@link #createUnderlyingMapWithCapacity(int)}.
         */
        RowRedirection createRowRedirection(TLongLongMap map);
    }

    Factory FACTORY = new RowRedirectionLockFreeFactory();
}
