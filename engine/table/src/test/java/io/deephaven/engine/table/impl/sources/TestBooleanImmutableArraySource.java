package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestBooleanImmutableArraySource extends AbstractBooleanColumnSourceTest {
    @NotNull
    @Override
    WritableColumnSource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableByteArraySource immutableBooleanArraySource = new ImmutableByteArraySource();
        immutableBooleanArraySource.ensureCapacity(capacity);
        final WritableByteAsBooleanColumnSource byteAsBooleanColumnSource = new WritableByteAsBooleanColumnSource(immutableBooleanArraySource);

        try (final ChunkSink.FillFromContext ffc = byteAsBooleanColumnSource.makeFillFromContext(capacity);
             final WritableObjectChunk nullChunk = WritableObjectChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            byteAsBooleanColumnSource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return byteAsBooleanColumnSource;
    }
}