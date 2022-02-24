/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutable2DArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DDoubleArraySource;
import org.jetbrains.annotations.NotNull;

public class TestDoubleImmutable2DArraySource extends AbstractDoubleColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Immutable2DDoubleArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Immutable2DDoubleArraySource flatDoubleArraySource = new Immutable2DDoubleArraySource(12);
        flatDoubleArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatDoubleArraySource.makeFillFromContext(capacity);
             final WritableDoubleChunk nullChunk = WritableDoubleChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatDoubleArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatDoubleArraySource;
    }
}