/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutable2DArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestByteImmutable2DArraySource extends AbstractByteColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Immutable2DByteArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Immutable2DByteArraySource flatByteArraySource = new Immutable2DByteArraySource(12);
        flatByteArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatByteArraySource.makeFillFromContext(capacity);
             final WritableByteChunk nullChunk = WritableByteChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatByteArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatByteArraySource;
    }
}