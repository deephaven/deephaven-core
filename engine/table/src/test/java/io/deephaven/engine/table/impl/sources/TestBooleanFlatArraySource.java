package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.flat.FlatByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestBooleanFlatArraySource extends AbstractBooleanColumnSourceTest {
    @NotNull
    @Override
    WritableColumnSource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatByteArraySource flatBooleanArraySource = new FlatByteArraySource(capacity);
        final WritableByteAsBooleanColumnSource byteAsBooleanColumnSource = new WritableByteAsBooleanColumnSource(flatBooleanArraySource);

        try (final ChunkSink.FillFromContext ffc = byteAsBooleanColumnSource.makeFillFromContext(capacity);
             final WritableObjectChunk nullChunk = WritableObjectChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            byteAsBooleanColumnSource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return byteAsBooleanColumnSource;
    }
}