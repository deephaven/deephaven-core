/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutableArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import org.jetbrains.annotations.NotNull;

public class TestIntegerImmutableArraySource extends AbstractIntegerColumnSourceTest {
    @NotNull
    @Override
    ImmutableIntArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableIntArraySource immutableIntArraySource = new ImmutableIntArraySource();
        immutableIntArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableIntArraySource.makeFillFromContext(capacity);
             final WritableIntChunk nullChunk = WritableIntChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableIntArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableIntArraySource;
    }
}