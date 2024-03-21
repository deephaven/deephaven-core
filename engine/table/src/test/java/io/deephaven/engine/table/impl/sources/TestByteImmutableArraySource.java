//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterImmutableArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestByteImmutableArraySource extends AbstractByteColumnSourceTest {
    @NotNull
    @Override
    ImmutableByteArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableByteArraySource immutableByteArraySource = new ImmutableByteArraySource();
        immutableByteArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableByteArraySource.makeFillFromContext(capacity);
                final WritableByteChunk nullChunk = WritableByteChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableByteArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableByteArraySource;
    }
}
