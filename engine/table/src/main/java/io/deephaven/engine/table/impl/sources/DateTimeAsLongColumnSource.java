/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link DateTime} to {@code long} values.
 */
public class DateTimeAsLongColumnSource extends AbstractColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {

    private final ColumnSource<DateTime> alternateColumnSource;

    public DateTimeAsLongColumnSource(@NotNull final ColumnSource<DateTime> alternateColumnSource) {
        super(long.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public long getLong(final long index) {
        return DateTimeUtils.nanos(alternateColumnSource.get(index));
    }

    @Override
    public long getPrevLong(final long index) {
        return DateTimeUtils.nanos(alternateColumnSource.getPrev(index));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == DateTime.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        //noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    private class UnboxingFillContext implements FillContext {
        final GetContext alternateGetContext;

        private UnboxingFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateGetContext = alternateColumnSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            alternateGetContext.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new UnboxingFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final UnboxingFillContext unboxingFillContext = (UnboxingFillContext) context;
        final ObjectChunk<DateTime, ? extends Values> dateTimeChunk = alternateColumnSource.getChunk(unboxingFillContext.alternateGetContext, rowSequence).asObjectChunk();
        convertToLong(destination, dateTimeChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final UnboxingFillContext unboxingFillContext = (UnboxingFillContext) context;
        final ObjectChunk<DateTime, ? extends Values> dateTimeChunk = alternateColumnSource.getPrevChunk(unboxingFillContext.alternateGetContext, rowSequence).asObjectChunk();
        convertToLong(destination, dateTimeChunk);
    }

    private static void convertToLong(@NotNull final WritableChunk<? super Values> destination, @NotNull final ObjectChunk<DateTime, ? extends Values> dateTimeChunk) {
        final WritableLongChunk<? super Values> longDestination = destination.asWritableLongChunk();
        for (int ii = 0; ii < dateTimeChunk.size(); ++ii) {
            final DateTime dateTime = dateTimeChunk.get(ii);
            longDestination.set(ii, DateTimeUtils.nanos(dateTime));
        }
        longDestination.setSize(dateTimeChunk.size());
    }

    @Override
    public boolean preventsParallelism() {
        return alternateColumnSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return alternateColumnSource.isStateless();
    }
}
