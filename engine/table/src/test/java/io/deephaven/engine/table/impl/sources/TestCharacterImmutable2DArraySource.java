package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DCharArraySource;
import org.jetbrains.annotations.NotNull;

public class TestCharacterImmutable2DArraySource extends AbstractCharacterColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Immutable2DCharArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Immutable2DCharArraySource flatCharArraySource = new Immutable2DCharArraySource(12);
        flatCharArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatCharArraySource.makeFillFromContext(capacity);
             final WritableCharChunk nullChunk = WritableCharChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatCharArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatCharArraySource;
    }
}