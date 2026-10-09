//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

public abstract class FixedWidthObjectChunkWriter<T> extends FixedWidthChunkWriter<ObjectChunk<T, Values>> {

    public FixedWidthObjectChunkWriter(
            final int elementSize,
            final boolean dhNullable,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, elementSize, dhNullable, fieldNullable);
    }

    @Override
    protected void computeValidity(
            @NotNull final BaseChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final ValidityBuffer validity) {
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> validity.setNextIsNull(objectChunk.isNull((int) row)));
    }
}
