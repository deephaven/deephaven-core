/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link long} to {@code DateTime} values.
 */
public class LongAsDateTimeColumnSource extends AbstractColumnSource<DateTime> implements MutableColumnSourceGetDefaults.ForObject<DateTime>, FillUnordered<Values> {

    private final ColumnSource<Long> alternateColumnSource;

    public LongAsDateTimeColumnSource(ColumnSource<Long> alternateColumnSource) {
        super(DateTime.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public DateTime get(final long rowKey) {
        final long longValue = alternateColumnSource.getLong(rowKey);
        return DateTimeUtils.nanosToTime(longValue);
    }

    @Override
    public DateTime getPrev(final long rowKey) {
        final long longValue = alternateColumnSource.getPrevLong(rowKey);
        return DateTimeUtils.nanosToTime(longValue);
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Long.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        //noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    private class ToDateTimeFillContext implements FillContext {
        final GetContext alternateGetContext;
        final FillContext alternateFillContext;
        final WritableLongChunk<Values> longChunk;

        private ToDateTimeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateGetContext = alternateColumnSource.makeGetContext(chunkCapacity, sharedContext);
            if (providesFillUnordered()) {
                alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
                longChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
            } else {
                alternateFillContext = null;
                longChunk = null;
            }
        }

        @Override
        public void close() {
            alternateGetContext.close();
            if (alternateFillContext != null) {
                alternateFillContext.close();
                longChunk.close();
            }
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToDateTimeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        final LongChunk<? extends Values> longChunk = alternateColumnSource.getChunk(toDateTimeFillContext.alternateGetContext, rowSequence).asLongChunk();
        convertToDateTime(destination, longChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        final LongChunk<? extends Values> longChunk = alternateColumnSource.getPrevChunk(toDateTimeFillContext.alternateGetContext, rowSequence).asLongChunk();
        convertToDateTime(destination, longChunk);
    }

    @Override
    public boolean providesFillUnordered() {
        return FillUnordered.providesFillUnordered(alternateColumnSource);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        if (toDateTimeFillContext.longChunk == null) {
            throw new UnsupportedOperationException("Unordered fill is not supported by this column source!");
        }
        toDateTimeFillContext.longChunk.setSize(keys.size());
        //noinspection unchecked
        ((FillUnordered<Values>) alternateColumnSource).fillChunkUnordered(toDateTimeFillContext.alternateFillContext, toDateTimeFillContext.longChunk, keys);
        convertToDateTime(dest, toDateTimeFillContext.longChunk);
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        if (toDateTimeFillContext.longChunk == null) {
            throw new UnsupportedOperationException("Unordered fill is not supported by this column source!");
        }
        toDateTimeFillContext.longChunk.setSize(keys.size());
        //noinspection unchecked
        ((FillUnordered<Values>) alternateColumnSource).fillPrevChunkUnordered(toDateTimeFillContext.alternateFillContext, toDateTimeFillContext.longChunk, keys);
        convertToDateTime(dest, toDateTimeFillContext.longChunk);
    }

    private static void convertToDateTime(@NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends Values> longChunk) {
        final WritableObjectChunk<DateTime, ? super Values> dateTimeObjectDestination = destination.asWritableObjectChunk();
        for (int ii = 0; ii < longChunk.size(); ++ii) {
            final long longValue = longChunk.get(ii);
            dateTimeObjectDestination.set(ii, DateTimeUtils.nanosToTime(longValue));
        }
        dateTimeObjectDestination.setSize(longChunk.size());
    }

    @Override
    public boolean isStateless() {
        return alternateColumnSource.isStateless();
    }
}
