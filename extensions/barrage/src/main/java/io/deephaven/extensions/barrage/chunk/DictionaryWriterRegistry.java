//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Manages the set of {@link DictionaryWriterState} instances for a single barrage stream (snapshot or subscription
 * update sequence), one per Arrow dictionary id.
 *
 * <p>
 * Two construction modes:
 * <ul>
 * <li><b>Local</b> (default constructor) — for viewport subscriptions and snapshots. Each registry creates
 * {@link LocalDictionaryWriterState} instances that are fully private to this stream.</li>
 * <li><b>Shared-backed</b> ({@link #DictionaryWriterRegistry(Long2ObjectOpenHashMap)} constructor) — for full
 * subscriptions and growing subscriptions targeting a full subscription. Each registry creates
 * {@link SharedDictionaryWriterState} instances that delegate index lookups to the table-level
 * {@link SharedWriterDictionary}, so all full subscribers share the same value-to-index mapping.</li>
 * </ul>
 *
 * <p>
 * When {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} processes a batch, it calls
 * {@link #getOrCreate} for each {@link DictionaryChunkWriter} column to obtain (or register) the state for that
 * column's dictionary id. After the batch's column data is serialized, the manager's {@link #entries()} are inspected
 * to determine which dictionary ids have pending deltas that need a {@link org.apache.arrow.flatbuf.DictionaryBatch}
 * message.
 */
public final class DictionaryWriterRegistry {

    /** Per-id state plus the values writer needed to serialize dictionary values into a DictionaryBatch. */
    public static final class Entry {
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
     * If non-null, this registry creates {@link SharedDictionaryWriterState} instances backed by the shared states in
     * this map. If null, it creates standalone {@link LocalDictionaryWriterState} instances.
     */
    @Nullable
    private final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries;

    private final Long2ObjectOpenHashMap<Entry> entries = new Long2ObjectOpenHashMap<>();

    /** Creates a local registry for viewport subscriptions and snapshots. */
    public DictionaryWriterRegistry() {
        this.sharedDictionaries = null;
    }

    /**
     * Creates a shared-backed registry for full subscriptions and growing subscriptions targeting a full subscription.
     * The {@code sharedDictionaries} map is owned by the {@code io.deephaven.server.barrage.BarrageMessageProducer} and
     * lives for the lifetime of the table's producer; it is never cleared.
     */
    public DictionaryWriterRegistry(
            @NotNull final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries) {
        this.sharedDictionaries = sharedDictionaries;
    }

    /**
     * Returns (or creates) the {@link DictionaryWriterState} for the given dictionary id.
     *
     * <p>
     * The first call for a given {@code dictId} registers the {@code valuesWriter} for that id and creates the
     * appropriate state type (local or shared-backed). Subsequent calls for the same id return the existing state.
     */
    @NotNull
    public DictionaryWriterState getOrCreate(
            final long dictId,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull final ChunkType valuesChunkType) {
        Entry entry = entries.get(dictId);
        if (entry == null) {
            final DictionaryWriterState state;
            if (sharedDictionaries != null) {
                SharedWriterDictionary shared = sharedDictionaries.get(dictId);
                if (shared == null) {
                    shared = new SharedWriterDictionary(dictId, valuesChunkType);
                    sharedDictionaries.put(dictId, shared);
                }
                state = new SharedDictionaryWriterState(shared);
            } else {
                state = new LocalDictionaryWriterState(dictId, valuesChunkType);
            }
            entry = new Entry(state, valuesWriter);
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
        return entries.values().stream().anyMatch(e -> e.state.hasDelta());
    }

    /** Advances the delta boundary for every registered state that currently has a pending delta. */
    public void resetDeltas() {
        for (final Entry e : entries.values()) {
            if (e.state.hasDelta()) {
                e.state.resetDelta();
            }
        }
    }

    /**
     * Checks each registered {@link DictionaryWriterState} for overflow: if its
     * {@link DictionaryWriterState#totalSize() totalSize} exceeds {@code liveRowCount}, calls
     * {@link DictionaryWriterState#reset()} so the next DictionaryBatch will be {@code isDelta=false} with a compacted
     * dictionary. Only call this for local (viewport/snapshot) registries; for shared-backed registries the
     * {@link SharedWriterDictionary} is reset externally.
     *
     * @param liveRowCount the current number of live rows visible to this subscription
     */
    public void resetOverflowedEntries(final long liveRowCount) {
        for (final Entry e : entries.values()) {
            if (e.state.totalSize() > liveRowCount) {
                e.state.reset();
            }
        }
    }
}
