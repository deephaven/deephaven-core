/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.Context;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import gnu.trove.map.TLongLongMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * A RedirectionIndex can be in one of two states: tracking prev values or not. The typical lifecycle looks like this:
 * <ol>
 * <li>A RedirectionIndex is created with an initial map, but not tracking prev values. In this state, get() and
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
public interface RedirectionIndex {
    long put(long key, long index);


    long get(long key);


    interface FillContext extends Context {
    }

    FillContext DEFAULT_FILL_INSTANCE = new FillContext() {};

    default FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return DEFAULT_FILL_INSTANCE;
    }

    /**
     * Lookup each element in OrderedKeys and write the result to mappedKeysOut
     *
     * @param fillContext the RedirectionIndex FillContext
     * @param mappedKeysOut the result chunk
     * @param keysToMap the keys to lookup in this redirection index
     */
    default void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        // Assume that caller provided a chunk large enough to use.
        mappedKeysOut.setSize(0);
        keysToMap.forEachLong((final long k) -> {
            mappedKeysOut.add(get(k));
            return true;
        });
    }

    default void fillChunkUnordered(
            @NotNull final FillContext fillContext,
            @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
            @NotNull final LongChunk<KeyIndices> keysToMap) {
        // Assume that caller provided a chunk large enough to use.
        mappedKeysOut.setSize(0);
        for (int ii = 0; ii < keysToMap.size(); ++ii) {
            mappedKeysOut.add(get(keysToMap.get(ii)));
        }
    }

    default void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        // Assume that caller provided a chunk large enough to use.
        mappedKeysOut.setSize(0);
        keysToMap.forEachLong((final long k) -> {
            mappedKeysOut.add(getPrev(k));
            return true;
        });
    }

    long getPrev(long key);

    long remove(long leftIndex);

    void startTrackingPrevValues();

    /**
     * Like put, but we do not care about a return value.
     *
     * @param key the key to put
     * @param index the inner value to insert into the redirection index
     */
    default void putVoid(long key, long index) {
        put(key, index);
    }

    /**
     * Like remove, but we do not care about a return value.
     *
     * @param key the key to remove
     */
    default void removeVoid(long key) {
        remove(key);
    }

    default void removeAll(final OrderedKeys keys) {
        keys.forAllLongs(this::remove);
    }

    /**
     * Provide a default, empty {@link WritableChunkSink.FillFromContext} for use with our default
     * {@link WritableSource#fillFromChunk}.
     */
    default WritableChunkSink.FillFromContext makeFillFromContext(int chunkCapacity) {
        // chunkCapacity ignored
        return EMPTY_CONTEXT;
    }

    WritableChunkSink.FillFromContext EMPTY_CONTEXT = new WritableChunkSink.FillFromContext() {};

    /**
     * Our default, inefficient, implementation. Inheritors who care should provide a better implementation.
     */
    default void fillFromChunk(@NotNull WritableChunkSink.FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull OrderedKeys orderedKeys) {
        final MutableInt offset = new MutableInt();
        final LongChunk<? extends Values> valuesLongChunk = src.asLongChunk();
        orderedKeys.forAllLongs(key -> {
            final long index = valuesLongChunk.get(offset.intValue());
            if (index == Index.NULL_KEY) {
                removeVoid(key);
            } else {
                putVoid(key, index);
            }
            offset.increment();
        });
    }

    /**
     * Update this RedirectionIndex according to the IndexShiftData.
     *
     * @param tableIndex an Index to filter which rows should be shifted
     * @param shiftData the IndexShiftData for this update
     */
    default void applyShift(final ReadOnlyIndex tableIndex, final IndexShiftData shiftData) {
        RedirectionIndexUtilities.applyRedirectionShift(this, tableIndex, shiftData);
    }

    interface Factory {
        TLongLongMap createUnderlyingMapWithCapacity(int initialCapacity);

        RedirectionIndex createRedirectionIndex(int initialCapacity);

        /**
         * @param map The initial map. Needs to have the same dynamic type as that returned by
         *        {@link #createUnderlyingMapWithCapacity(int)}.
         */
        RedirectionIndex createRedirectionIndex(TLongLongMap map);
    }

    String USE_LOCK_FREE_IMPL_PROPERTY_NAME = RedirectionIndex.class.getSimpleName() + "." + "useLockFreeImpl";

    Factory FACTORY = new RedirectionIndexLockFreeFactory();
}
