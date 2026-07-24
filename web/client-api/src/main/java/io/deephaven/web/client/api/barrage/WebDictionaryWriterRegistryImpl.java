//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterState;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;

/**
 * The JS API's {@link DictionaryWriterRegistry}. The JS API does not yet write dictionary-encoded columns, so this
 * registry never has any entries; the server implementation is backed by fastutil maps that are not available under
 * GWT.
 */
public final class WebDictionaryWriterRegistryImpl implements DictionaryWriterRegistry {

    @Override
    @NotNull
    public DictionaryWriterState getOrCreate(
            final long dictId,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull final ChunkType valuesChunkType) {
        throw new UnsupportedOperationException("Dictionary encoding is not supported when writing from the JS API");
    }

    @Override
    @NotNull
    public Collection<Entry> entries() {
        return Collections.emptyList();
    }

    @Override
    public boolean hasAnyDelta() {
        return false;
    }

    @Override
    public void resetDeltas() {}

    @Override
    public void resetOverflowedEntries(final long liveRowCount) {}
}
