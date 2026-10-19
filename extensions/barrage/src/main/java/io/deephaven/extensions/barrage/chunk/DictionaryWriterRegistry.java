//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.LongUnaryOperator;

/**
 * Manages the set of {@link DictionaryWriterState} instances for a single barrage stream (snapshot or subscription
 * update sequence), one per Arrow dictionary id.
 *
 * <p>
 * When {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} processes a batch, it calls
 * {@link #getOrCreate} for each {@link DictionaryChunkWriter} column to obtain (or register) the state for that
 * column's dictionary id. After the batch's column data is serialized, the registry's {@link #entries()} are inspected
 * to determine which dictionary ids have pending deltas that need a {@link org.apache.arrow.flatbuf.DictionaryBatch}
 * message.
 *
 * <p>
 * The server implementation is {@link DictionaryWriterRegistryImpl}. The JS API supplies its own implementation, since
 * the server implementation's fastutil backing is not available under GWT.
 */
public interface DictionaryWriterRegistry {

    /** Per-id state plus the values writer needed to serialize dictionary values into a DictionaryBatch. */
    final class Entry {
        public final DictionaryWriterState state;
        public final ChunkWriter<Chunk<Values>> valuesWriter;

        Entry(
                @NotNull final DictionaryWriterState state,
                @NotNull final ChunkWriter<Chunk<Values>> valuesWriter) {
            this.state = state;
            this.valuesWriter = valuesWriter;
        }
    }

    /**
     * Returns (or creates) the {@link DictionaryWriterState} for the given dictionary id.
     *
     * <p>
     * The first call for a given {@code dictId} registers the {@code valuesWriter} for that id and creates the
     * appropriate state type. Subsequent calls for the same id return the existing state.
     */
    @NotNull
    DictionaryWriterState getOrCreate(
            long dictId,
            @NotNull ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull ChunkType valuesChunkType);

    /** Returns all registered entries. */
    @NotNull
    Collection<Entry> entries();

    /** Returns {@code true} if any registered dictionary has a pending delta that needs a DictionaryBatch message. */
    boolean hasAnyDelta();

    /** Advances the delta boundary for every registered state that currently has a pending delta. */
    void resetDeltas();

    /**
     * Resets each registered {@link DictionaryWriterState} whose {@link DictionaryWriterState#totalSize() totalSize}
     * exceeds the limit {@code maxSizeForDictId} gives for its dictionary id, so the next DictionaryBatch will be
     * {@code isDelta=false} with a compacted dictionary. This applies to local (viewport/snapshot) registries only; a
     * shared-backed registry ignores this call, as its {@link SharedWriterDictionary} is reset externally.
     *
     * @param maxSizeForDictId the most values the dictionary with a given id may hold before it is reset; typically no
     *        more than the live row count visible to the subscription
     */
    void maybeResetEntries(@NotNull LongUnaryOperator maxSizeForDictId);
}
