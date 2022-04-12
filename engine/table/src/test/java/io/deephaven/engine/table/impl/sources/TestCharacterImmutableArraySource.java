package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import org.jetbrains.annotations.NotNull;

public class TestCharacterImmutableArraySource extends AbstractCharacterColumnSourceTest {
    @NotNull
    @Override
    ImmutableCharArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableCharArraySource immutableCharArraySource = new ImmutableCharArraySource();
        immutableCharArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableCharArraySource.makeFillFromContext(capacity);
             final WritableCharChunk nullChunk = WritableCharChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableCharArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableCharArraySource;
    }
}