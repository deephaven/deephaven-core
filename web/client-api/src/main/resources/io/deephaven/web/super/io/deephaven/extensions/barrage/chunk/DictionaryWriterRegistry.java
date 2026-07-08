//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;

/**
 * The JS API never writes dictionary-encoded columns, so this registry never has any entries; it exists
 * only so that {@code BarrageMessageWriterImpl} compiles under GWT. The server implementation is backed by fastutil
 * maps and creates real {@link DictionaryWriterState} instances.
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

    public DictionaryWriterRegistry() {}

    @NotNull
    public DictionaryWriterState getOrCreate(
            final long dictId,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull final ChunkType valuesChunkType) {
        throw new UnsupportedOperationException("Dictionary encoding is not supported when writing from the JS API");
    }

    /** Returns all registered entries; always empty in the browser. */
    @NotNull
    public Collection<Entry> entries() {
        return Collections.emptyList();
    }

    /** Returns {@code true} if any registered dictionary has a pending delta; always {@code false} in the browser. */
    public boolean hasAnyDelta() {
        return false;
    }

    /** No-op in the browser. */
    public void resetDeltas() {}

    /** No-op in the browser. */
    public void resetOverflowedEntries(final long liveRowCount) {}
}
