/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterFlat2DArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.flat.Flat2DShortArraySource;
import io.deephaven.engine.table.impl.sources.flat.FlatShortArraySource;
import org.jetbrains.annotations.NotNull;

public class TestShortFlat2DArraySource extends AbstractShortColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Flat2DShortArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Flat2DShortArraySource flatShortArraySource = new Flat2DShortArraySource(12);
        flatShortArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatShortArraySource.makeFillFromContext(capacity);
             final WritableShortChunk nullChunk = WritableShortChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatShortArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatShortArraySource;
    }
}