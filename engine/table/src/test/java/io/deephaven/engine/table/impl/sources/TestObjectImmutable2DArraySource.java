package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DObjectArraySource;
import org.jetbrains.annotations.NotNull;

public class TestObjectImmutable2DArraySource extends AbstractObjectColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Immutable2DObjectArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Immutable2DObjectArraySource<String> immutable2DObjectArraySource = new Immutable2DObjectArraySource<>(String.class, null, 12);
        immutable2DObjectArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutable2DObjectArraySource.makeFillFromContext(capacity);
             final WritableObjectChunk<?, Values> nullChunk = WritableObjectChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutable2DObjectArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutable2DObjectArraySource;
    }
}