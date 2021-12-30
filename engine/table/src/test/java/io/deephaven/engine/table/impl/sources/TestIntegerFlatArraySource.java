/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlatArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatIntArraySource;
import org.jetbrains.annotations.NotNull;

public class TestIntegerFlatArraySource extends AbstractIntegerColumnSourceTest {
    @NotNull
    @Override
    FlatIntArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatIntArraySource flatIntArraySource = new FlatIntArraySource(capacity);
        try (final ChunkSink.FillFromContext ffc = flatIntArraySource.makeFillFromContext(capacity);
             final WritableIntChunk nullChunk = WritableIntChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatIntArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatIntArraySource;
    }
}