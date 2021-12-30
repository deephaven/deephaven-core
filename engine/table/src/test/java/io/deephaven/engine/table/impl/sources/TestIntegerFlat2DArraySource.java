/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlat2DArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DIntArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatIntArraySource;
import org.jetbrains.annotations.NotNull;

public class TestIntegerFlat2DArraySource extends AbstractIntegerColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DIntArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DIntArraySource flatIntArraySource = new Flat2DIntArraySource(capacity, 12);
        try (final ChunkSink.FillFromContext ffc = flatIntArraySource.makeFillFromContext(capacity);
             final WritableIntChunk nullChunk = WritableIntChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatIntArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatIntArraySource;
    }
}