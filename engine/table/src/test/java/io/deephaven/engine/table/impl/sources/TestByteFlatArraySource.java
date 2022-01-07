/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlatArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestByteFlatArraySource extends AbstractByteColumnSourceTest {
    @NotNull
    @Override
    FlatByteArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatByteArraySource flatByteArraySource = new FlatByteArraySource();
        flatByteArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatByteArraySource.makeFillFromContext(capacity);
             final WritableByteChunk nullChunk = WritableByteChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatByteArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatByteArraySource;
    }
}