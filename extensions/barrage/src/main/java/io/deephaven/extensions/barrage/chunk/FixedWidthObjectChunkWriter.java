//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

public abstract class FixedWidthObjectChunkWriter<T> extends FixedWidthChunkWriter<ObjectChunk<T, Values>> {

    public FixedWidthObjectChunkWriter(
            final int elementSize,
            final boolean dhNullable,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, elementSize, dhNullable, fieldNullable);
    }

    @Override
    protected int computeNullCount(
            @NotNull final BaseChunkWriter.Context context,
            @NotNull final RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        subset.forAllRowKeys(row -> {
            if (context.getChunk().asObjectChunk().isNull((int) row)) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final BaseChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        subset.forAllRowKeys(row -> {
            serContext.setNextIsNull(context.getChunk().asObjectChunk().isNull((int) row));
        });
    }
}
