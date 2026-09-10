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
 * The server-side {@link DictionaryWriterRegistry} implementation, backed by fastutil maps.
 *
 * <p>
 * Two construction modes:
 * <ul>
 * <li><b>Local</b> (default constructor) — for viewport subscriptions and snapshots. Each registry creates
 * {@link LocalDictionaryWriterState} instances that are fully private to this stream.</li>
 * <li><b>Shared-backed</b> ({@link #DictionaryWriterRegistryImpl(Long2ObjectOpenHashMap)} constructor) — for full
 * subscriptions and growing subscriptions targeting a full subscription. Each registry creates
 * {@link SharedDictionaryWriterState} instances that delegate index lookups to the table-level
 * {@link SharedWriterDictionary}, so all full subscribers share the same value-to-index mapping.</li>
 * </ul>
 */
public final class DictionaryWriterRegistryImpl implements DictionaryWriterRegistry {

    /**
     * If non-null, this registry creates {@link SharedDictionaryWriterState} instances backed by the shared states in
     * this map. If null, it creates standalone {@link LocalDictionaryWriterState} instances.
     */
    @Nullable
    private final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries;

    private final Long2ObjectOpenHashMap<Entry> entries = new Long2ObjectOpenHashMap<>();

    /** Creates a local registry for viewport subscriptions and snapshots. */
    public DictionaryWriterRegistryImpl() {
        this.sharedDictionaries = null;
    }

    /**
     * Creates a shared-backed registry for full subscriptions and growing subscriptions targeting a full subscription.
     * The {@code sharedDictionaries} map is owned by the {@code io.deephaven.server.barrage.BarrageMessageProducer} and
     * lives for the lifetime of the table's producer; it is never cleared.
     */
    public DictionaryWriterRegistryImpl(
            @NotNull final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries) {
        this.sharedDictionaries = sharedDictionaries;
    }

    @Override
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

    @Override
    @NotNull
    public Collection<Entry> entries() {
        return entries.values();
    }

    @Override
    public boolean hasAnyDelta() {
        return entries.values().stream().anyMatch(e -> e.state.hasDelta());
    }

    @Override
    public void resetDeltas() {
        for (final Entry e : entries.values()) {
            if (e.state.hasDelta()) {
                e.state.resetDelta();
            }
        }
    }

    @Override
    public void resetOverflowedEntries(final long liveRowCount) {
        for (final Entry e : entries.values()) {
            if (e.state.totalSize() > liveRowCount) {
                e.state.reset();
            }
        }
    }
}
