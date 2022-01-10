/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutable2DArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DIntArraySource;
import org.jetbrains.annotations.NotNull;

public class TestIntegerImmutable2DArraySource extends AbstractIntegerColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Immutable2DIntArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Immutable2DIntArraySource flatIntArraySource = new Immutable2DIntArraySource(12);
        flatIntArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatIntArraySource.makeFillFromContext(capacity);
             final WritableIntChunk nullChunk = WritableIntChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatIntArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatIntArraySource;
    }
}