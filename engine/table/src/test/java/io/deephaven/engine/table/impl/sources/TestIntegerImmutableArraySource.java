//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterImmutableArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import org.jetbrains.annotations.NotNull;

public class TestIntegerImmutableArraySource extends AbstractIntegerColumnSourceTest {
    @NotNull
    @Override
    ImmutableIntArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableIntArraySource immutableIntArraySource = new ImmutableIntArraySource();
        immutableIntArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableIntArraySource.makeFillFromContext(capacity);
                final WritableIntChunk nullChunk = WritableIntChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableIntArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableIntArraySource;
    }
}
