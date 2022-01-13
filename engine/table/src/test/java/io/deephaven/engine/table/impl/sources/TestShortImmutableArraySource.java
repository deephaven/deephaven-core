/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutableArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableShortArraySource;
import org.jetbrains.annotations.NotNull;

public class TestShortImmutableArraySource extends AbstractShortColumnSourceTest {
    @NotNull
    @Override
    ImmutableShortArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableShortArraySource immutableShortArraySource = new ImmutableShortArraySource();
        immutableShortArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableShortArraySource.makeFillFromContext(capacity);
             final WritableShortChunk nullChunk = WritableShortChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableShortArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableShortArraySource;
    }
}