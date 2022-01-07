/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlatArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatFloatArraySource;
import org.jetbrains.annotations.NotNull;

public class TestFloatFlatArraySource extends AbstractFloatColumnSourceTest {
    @NotNull
    @Override
    FlatFloatArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatFloatArraySource flatFloatArraySource = new FlatFloatArraySource();
        flatFloatArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatFloatArraySource.makeFillFromContext(capacity);
             final WritableFloatChunk nullChunk = WritableFloatChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatFloatArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatFloatArraySource;
    }
}