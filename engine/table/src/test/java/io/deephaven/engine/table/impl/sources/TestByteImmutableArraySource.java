/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutableArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import org.jetbrains.annotations.NotNull;

public class TestByteImmutableArraySource extends AbstractByteColumnSourceTest {
    @NotNull
    @Override
    ImmutableByteArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableByteArraySource immutableByteArraySource = new ImmutableByteArraySource();
        immutableByteArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableByteArraySource.makeFillFromContext(capacity);
             final WritableByteChunk nullChunk = WritableByteChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableByteArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableByteArraySource;
    }
}