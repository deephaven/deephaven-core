/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlat2DArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DLongArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatLongArraySource;
import org.jetbrains.annotations.NotNull;

public class TestLongFlat2DArraySource extends AbstractLongColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DLongArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DLongArraySource flatLongArraySource = new Flat2DLongArraySource(12);
        flatLongArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatLongArraySource.makeFillFromContext(capacity);
             final WritableLongChunk nullChunk = WritableLongChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatLongArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatLongArraySource;
    }
}