/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterImmutableArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import org.jetbrains.annotations.NotNull;

public class TestDoubleImmutableArraySource extends AbstractDoubleColumnSourceTest {
    @NotNull
    @Override
    ImmutableDoubleArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final ImmutableDoubleArraySource immutableDoubleArraySource = new ImmutableDoubleArraySource();
        immutableDoubleArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = immutableDoubleArraySource.makeFillFromContext(capacity);
             final WritableDoubleChunk nullChunk = WritableDoubleChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            immutableDoubleArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return immutableDoubleArraySource;
    }
}