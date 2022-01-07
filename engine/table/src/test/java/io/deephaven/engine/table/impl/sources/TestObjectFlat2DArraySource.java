package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DObjectArraySource;
import org.jetbrains.annotations.NotNull;

public class TestObjectFlat2DArraySource extends AbstractObjectColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DObjectArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DObjectArraySource<String> flat2DObjectArraySource = new Flat2DObjectArraySource<>(String.class, null, 12);
        flat2DObjectArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flat2DObjectArraySource.makeFillFromContext(capacity);
             final WritableObjectChunk<?, Values> nullChunk = WritableObjectChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flat2DObjectArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flat2DObjectArraySource;
    }
}