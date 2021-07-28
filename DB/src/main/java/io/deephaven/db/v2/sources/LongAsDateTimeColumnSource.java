/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link long} to {@code DBDateTime} values.
 */
@AbstractColumnSource.IsSerializable(value = true)
public class LongAsDateTimeColumnSource extends AbstractColumnSource<DBDateTime> implements MutableColumnSourceGetDefaults.ForObject<DBDateTime> {

    private final ColumnSource<Long> alternateColumnSource;

    public LongAsDateTimeColumnSource(ColumnSource<Long> alternateColumnSource) {
        super(DBDateTime.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public DBDateTime get(final long index) {
        final long longValue = alternateColumnSource.getLong(index);
        return DBTimeUtils.nanosToTime(longValue);
    }

    @Override
    public DBDateTime getPrev(final long index) {
        final long longValue = alternateColumnSource.getPrevLong(index);
        return DBTimeUtils.nanosToTime(longValue);
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

        private ToDateTimeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateGetContext = alternateColumnSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            alternateGetContext.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToDateTimeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        final LongChunk<? extends Values> longChunk = alternateColumnSource.getChunk(toDateTimeFillContext.alternateGetContext, orderedKeys).asLongChunk();
        convertToDBDateTime(destination, longChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        final LongChunk<? extends Values> longChunk = alternateColumnSource.getPrevChunk(toDateTimeFillContext.alternateGetContext, orderedKeys).asLongChunk();
        convertToDBDateTime(destination, longChunk);
    }

    private static void convertToDBDateTime(@NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends Values> longChunk) {
        final WritableObjectChunk<DBDateTime, ? super Values> dBDateTimeObjectDestination = destination.asWritableObjectChunk();
        for (int ii = 0; ii < longChunk.size(); ++ii) {
            final long longValue = longChunk.get(ii);
            dBDateTimeObjectDestination.set(ii, DBTimeUtils.nanosToTime(longValue));
        }
        dBDateTimeObjectDestination.setSize(longChunk.size());
    }
}