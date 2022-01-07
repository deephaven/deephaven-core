package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DCharArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatCharArraySource;
import org.jetbrains.annotations.NotNull;

public class TestCharacterFlat2DArraySource extends AbstractCharacterColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DCharArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DCharArraySource flatCharArraySource = new Flat2DCharArraySource(12);
        flatCharArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatCharArraySource.makeFillFromContext(capacity);
             final WritableCharChunk nullChunk = WritableCharChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatCharArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatCharArraySource;
    }
}