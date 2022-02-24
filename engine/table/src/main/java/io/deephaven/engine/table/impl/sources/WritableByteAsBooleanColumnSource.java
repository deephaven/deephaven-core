/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link byte} to {@code Boolean} values.
 */
public class WritableByteAsBooleanColumnSource extends ByteAsBooleanColumnSource implements MutableColumnSourceGetDefaults.ForBoolean, WritableColumnSource<Boolean> {

    private final WritableColumnSource<Byte> alternateColumnSource;

    public WritableByteAsBooleanColumnSource(@NotNull final WritableColumnSource<Byte> alternateColumnSource) {
        super(alternateColumnSource);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public void set(long key, Boolean value) {
        alternateColumnSource.set(key, BooleanUtils.booleanAsByte(value));
    }

    @Override
    public void set(long key, byte value) {
        alternateColumnSource.set(key, value);
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        alternateColumnSource.ensureCapacity(capacity, nullFilled);
    }

    private class ConvertingFillFromContext implements FillFromContext {
        private final FillFromContext alternateFillFromContext;
        private final WritableByteChunk<? extends Values> byteChunk;

        private ConvertingFillFromContext(int size) {
            alternateFillFromContext = alternateColumnSource.makeFillFromContext(size);
            byteChunk = WritableByteChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            alternateFillFromContext.close();
            byteChunk.close();
        }

        private void convert(ObjectChunk<Boolean, ? extends Values> src) {
            byteChunk.setSize(src.size());
            for (int ii = 0; ii < src.size(); ++ii) {
                byteChunk.set(ii, BooleanUtils.booleanAsByte(src.get(ii)));
            }
        }
    }

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return new ConvertingFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        final ConvertingFillFromContext convertingFillFromContext = (ConvertingFillFromContext)context;
        convertingFillFromContext.convert(src.asObjectChunk());
        alternateColumnSource.fillFromChunk(convertingFillFromContext.alternateFillFromContext, convertingFillFromContext.byteChunk, rowSequence);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final ConvertingFillFromContext convertingFillFromContext = (ConvertingFillFromContext)context;
        convertingFillFromContext.convert(src.asObjectChunk());
        alternateColumnSource.fillFromChunkUnordered(convertingFillFromContext.alternateFillFromContext, convertingFillFromContext.byteChunk, keys);
    }
}
