package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatObjectArraySource;
import org.jetbrains.annotations.NotNull;

public class TestObjectFlatArraySource extends AbstractObjectColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    FlatObjectArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatObjectArraySource<String> flatObjectArraySource = new FlatObjectArraySource<>(String.class, null, capacity);
        try (final ChunkSink.FillFromContext ffc = flatObjectArraySource.makeFillFromContext(capacity);
             final WritableObjectChunk<?, Values> nullChunk = WritableObjectChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatObjectArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatObjectArraySource;
    }
}