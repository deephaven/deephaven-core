/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlat2DArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DFloatArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatFloatArraySource;
import org.jetbrains.annotations.NotNull;

public class TestFloatFlat2DArraySource extends AbstractFloatColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DFloatArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DFloatArraySource flatFloatArraySource = new Flat2DFloatArraySource(capacity, 12);
        try (final ChunkSink.FillFromContext ffc = flatFloatArraySource.makeFillFromContext(capacity);
             final WritableFloatChunk nullChunk = WritableFloatChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatFloatArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatFloatArraySource;
    }
}