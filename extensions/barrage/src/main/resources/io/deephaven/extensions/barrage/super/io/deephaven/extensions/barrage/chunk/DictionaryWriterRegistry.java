//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;

import java.util.Collection;
import java.util.Collections;

/**
 * GWT super-source stub for {@code DictionaryWriterRegistry}.
 * <p>
 * Dictionary-encoded (DE) writing is <b>not supported</b> in the GWT/browser environment. This stub exists solely to
 * allow {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} to compile under GWT. Instances of this class
 * are created in GWT (e.g. lazily by {@code SnapshotView}), so methods that are reachable at runtime return safe
 * no-op values rather than throwing. {@link #getOrCreate} is the only method that is never reached in GWT (it is
 * guarded by an {@code instanceof DictionaryChunkWriter} check that always evaluates to {@code false}).
 */
public final class DictionaryWriterRegistry {

    /** Stub entry; fields are never populated in GWT. */
    public static final class Entry {
        public final DictionaryWriterState state = null;
        public final ChunkWriter<Chunk<Values>> valuesWriter = null;
        public final ChunkType valuesChunkType = null;
    }

    public DictionaryWriterRegistry() {}

    public DictionaryWriterState getOrCreate(
            final long dictId,
            final ChunkWriter<Chunk<Values>> valuesWriter,
            final ChunkType valuesChunkType) {
        return null;
    }

    public Collection<Entry> entries() {
        return Collections.emptyList();
    }

    public boolean hasAnyDelta() {
        return false;
    }

    public void resetDeltas() {}

    public void resetOverflowedEntries(final long liveRowCount) {}
}
