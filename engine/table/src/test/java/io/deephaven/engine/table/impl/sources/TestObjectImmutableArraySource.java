package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import org.jetbrains.annotations.NotNull;

public class TestObjectImmutableArraySource extends AbstractObjectColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    ImmutableObjectArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableObjectArraySource<String> immutableObjectArraySource = new ImmutableObjectArraySource<>(String.class, null);
        immutableObjectArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableObjectArraySource.makeFillFromContext(capacity);
             final WritableObjectChunk<?, Values> nullChunk = WritableObjectChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableObjectArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableObjectArraySource;
    }
}