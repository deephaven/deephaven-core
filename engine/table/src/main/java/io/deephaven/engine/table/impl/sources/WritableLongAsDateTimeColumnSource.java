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
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link byte} to {@code Boolean} values.
 */
public class WritableLongAsDateTimeColumnSource extends LongAsDateTimeColumnSource implements MutableColumnSourceGetDefaults.ForObject<DateTime>, WritableColumnSource<DateTime> {

    private final WritableColumnSource<Long> alternateColumnSource;

    public WritableLongAsDateTimeColumnSource(@NotNull final WritableColumnSource<Long> alternateColumnSource) {
        super(alternateColumnSource);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public void set(long key, DateTime value) {
        alternateColumnSource.set(key, DateTimeUtils.nanos(value));
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        alternateColumnSource.ensureCapacity(capacity, nullFilled);
    }

    private class ConvertingFillFromContext implements FillFromContext {
        private final FillFromContext alternateFillFromContext;
        private final WritableLongChunk<? extends Values> longChunk;

        private ConvertingFillFromContext(int size) {
            alternateFillFromContext = alternateColumnSource.makeFillFromContext(size);
            longChunk = WritableLongChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            alternateFillFromContext.close();
            longChunk.close();
        }

        private void convert(ObjectChunk<DateTime, ? extends Values> src) {
            longChunk.setSize(src.size());
            for (int ii = 0; ii < src.size(); ++ii) {
                longChunk.set(ii, DateTimeUtils.nanos(src.get(ii)));
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
        alternateColumnSource.fillFromChunk(convertingFillFromContext.alternateFillFromContext, convertingFillFromContext.longChunk, rowSequence);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final ConvertingFillFromContext convertingFillFromContext = (ConvertingFillFromContext)context;
        convertingFillFromContext.convert(src.asObjectChunk());
        alternateColumnSource.fillFromChunkUnordered(convertingFillFromContext.alternateFillFromContext, convertingFillFromContext.longChunk, keys);
    }
}
