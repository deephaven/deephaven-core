//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterImmutableArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableFloatArraySource;
import org.jetbrains.annotations.NotNull;

public class TestFloatImmutableArraySource extends AbstractFloatColumnSourceTest {
    @NotNull
    @Override
    ImmutableFloatArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableFloatArraySource immutableFloatArraySource = new ImmutableFloatArraySource();
        immutableFloatArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableFloatArraySource.makeFillFromContext(capacity);
                final WritableFloatChunk nullChunk = WritableFloatChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableFloatArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableFloatArraySource;
    }
}
