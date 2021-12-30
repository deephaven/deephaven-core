/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlatArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.FlatShortArraySource;
import org.jetbrains.annotations.NotNull;

public class TestShortFlatArraySource extends AbstractShortColumnSourceTest {
    @NotNull
    @Override
    FlatShortArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final FlatShortArraySource flatShortArraySource = new FlatShortArraySource(capacity);
        try (final ChunkSink.FillFromContext ffc = flatShortArraySource.makeFillFromContext(capacity);
             final WritableShortChunk nullChunk = WritableShortChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatShortArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatShortArraySource;
    }
}