/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlat2DArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DDoubleArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatDoubleArraySource;
import org.jetbrains.annotations.NotNull;

public class TestDoubleFlat2DArraySource extends AbstractDoubleColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DDoubleArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DDoubleArraySource flatDoubleArraySource = new Flat2DDoubleArraySource(capacity, 12);
        try (final ChunkSink.FillFromContext ffc = flatDoubleArraySource.makeFillFromContext(capacity);
             final WritableDoubleChunk nullChunk = WritableDoubleChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatDoubleArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatDoubleArraySource;
    }
}