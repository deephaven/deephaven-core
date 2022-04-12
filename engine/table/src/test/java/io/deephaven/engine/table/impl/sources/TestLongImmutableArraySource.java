/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutableArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import org.jetbrains.annotations.NotNull;

public class TestLongImmutableArraySource extends AbstractLongColumnSourceTest {
    @NotNull
    @Override
    ImmutableLongArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableLongArraySource immutableLongArraySource = new ImmutableLongArraySource();
        immutableLongArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableLongArraySource.makeFillFromContext(capacity);
             final WritableLongChunk nullChunk = WritableLongChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableLongArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableLongArraySource;
    }
}