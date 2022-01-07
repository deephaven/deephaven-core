/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlatArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatDoubleArraySource;
import org.jetbrains.annotations.NotNull;

public class TestDoubleFlatArraySource extends AbstractDoubleColumnSourceTest {
    @NotNull
    @Override
    FlatDoubleArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatDoubleArraySource flatDoubleArraySource = new FlatDoubleArraySource();
        flatDoubleArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatDoubleArraySource.makeFillFromContext(capacity);
             final WritableDoubleChunk nullChunk = WritableDoubleChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatDoubleArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatDoubleArraySource;
    }
}