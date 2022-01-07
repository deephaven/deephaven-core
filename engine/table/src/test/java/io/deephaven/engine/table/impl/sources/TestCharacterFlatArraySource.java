package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatCharArraySource;
import org.jetbrains.annotations.NotNull;

public class TestCharacterFlatArraySource extends AbstractCharacterColumnSourceTest {
    @NotNull
    @Override
    FlatCharArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatCharArraySource flatCharArraySource = new FlatCharArraySource();
        flatCharArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatCharArraySource.makeFillFromContext(capacity);
             final WritableCharChunk nullChunk = WritableCharChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatCharArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatCharArraySource;
    }
}