/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlatArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatLongArraySource;
import org.jetbrains.annotations.NotNull;

public class TestLongFlatArraySource extends AbstractLongColumnSourceTest {
    @NotNull
    @Override
    FlatLongArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatLongArraySource flatLongArraySource = new FlatLongArraySource(capacity);
        try (final ChunkSink.FillFromContext ffc = flatLongArraySource.makeFillFromContext(capacity);
             final WritableLongChunk nullChunk = WritableLongChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatLongArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatLongArraySource;
    }
}