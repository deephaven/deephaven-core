//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Manages the set of {@link DictionaryWriterState} instances for a single barrage stream (snapshot or subscription
 * update sequence), one per Arrow dictionary id.
 *
 * <p>
 * When {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} processes a batch, it calls
 * {@link #getOrCreate} for each {@link DictionaryChunkWriter} column to obtain (or register) the shared state for that
 * column's dictionary id. After the batch's column data is serialized, the manager's {@link #entries()} are inspected
 * to determine which dictionary ids have pending deltas that need a {@link org.apache.arrow.flatbuf.DictionaryBatch}
 * message.
 */
public final class DictionaryWriterRegistry {

    /** Per-id state plus the writer and chunk type needed to serialize dictionary values. */
    public static final class Entry {
        public final DictionaryWriterState state;
        public final ChunkWriter<Chunk<Values>> valuesWriter;
        public final ChunkType valuesChunkType;

        Entry(
                @NotNull final DictionaryWriterState state,
                @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
                @NotNull final ChunkType valuesChunkType) {
            this.state = state;
            this.valuesWriter = valuesWriter;
            this.valuesChunkType = valuesChunkType;
        }
    }

    private final Long2ObjectOpenHashMap<Entry> entries = new Long2ObjectOpenHashMap<>();

    /**
     * Returns (or creates) the {@link DictionaryWriterState} for the given dictionary id.
     *
     * <p>
     * The first call for a given {@code dictId} registers the {@code valuesWriter} for that id. Subsequent calls for
     * the same id may pass any consistent writer value.
     */
    @NotNull
    public DictionaryWriterState getOrCreate(
            final long dictId,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull final ChunkType valuesChunkType) {
        Entry entry = entries.get(dictId);
        if (entry == null) {
            entry = new Entry(new DictionaryWriterState(dictId), valuesWriter, valuesChunkType);
            entries.put(dictId, entry);
        }
        return entry.state;
    }

    /** Returns all registered entries. */
    @NotNull
    public Collection<Entry> entries() {
        return entries.values();
    }

    /** Returns {@code true} if any registered dictionary has a pending delta that needs a DictionaryBatch message. */
    public boolean hasAnyDelta() {
        for (final Entry e : entries.values()) {
            if (e.state.hasDelta()) {
                return true;
            }
        }
        return false;
    }

    /** Advances the delta boundary for every registered state that currently has a pending delta. */
    public void resetDeltas() {
        for (final Entry e : entries.values()) {
            if (e.state.hasDelta()) {
                e.state.resetDelta();
            }
        }
    }
}
