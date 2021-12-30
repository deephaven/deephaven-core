/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlat2DArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DByteArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestByteFlat2DArraySource extends AbstractByteColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DByteArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DByteArraySource flatByteArraySource = new Flat2DByteArraySource(capacity, 12);
        try (final ChunkSink.FillFromContext ffc = flatByteArraySource.makeFillFromContext(capacity);
             final WritableByteChunk nullChunk = WritableByteChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatByteArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatByteArraySource;
    }
}