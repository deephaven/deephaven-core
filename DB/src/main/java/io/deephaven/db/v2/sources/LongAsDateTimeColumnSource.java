/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.DBDateTime;
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
    public DBDateTime get(long index) {
        final long longValue = alternateColumnSource.getLong(index);
        if (longValue == QueryConstants.NULL_LONG) {
            return null;
        }
        return new DBDateTime(longValue);
    }

    @Override
    public DBDateTime getPrev(long index) {
        final long longValue = alternateColumnSource.getPrevLong(index);
        if (longValue == QueryConstants.NULL_LONG) {
            return null;
        }
        return new DBDateTime(longValue);
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
        final FillContext alternateFillContext;
        final WritableLongChunk<Values> longChunk;

        private ToDateTimeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
            longChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            alternateFillContext.close();
            longChunk.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToDateTimeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        final WritableLongChunk<Values> longChunk = toDateTimeFillContext.longChunk;
        alternateColumnSource.fillChunk(toDateTimeFillContext.alternateFillContext, longChunk, orderedKeys);
        convertToDBDateTime(destination, longChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final ToDateTimeFillContext toDateTimeFillContext = (ToDateTimeFillContext) context;
        final WritableLongChunk<Values> longChunk = toDateTimeFillContext.longChunk;
        alternateColumnSource.fillPrevChunk(toDateTimeFillContext.alternateFillContext, longChunk, orderedKeys);
        convertToDBDateTime(destination, longChunk);
    }

    private static void convertToDBDateTime(@NotNull WritableChunk<? super Values> destination, @NotNull LongChunk<? super Values> longChunk) {
        final WritableObjectChunk<DBDateTime, ? super Values> dBDateTimeObjectDestination = destination.asWritableObjectChunk();
        for (int ii = 0; ii < longChunk.size(); ++ii) {
            final long longValue = longChunk.get(ii);
            if (longValue == QueryConstants.NULL_LONG) {
                dBDateTimeObjectDestination.set(ii, null);
            } else {
                dBDateTimeObjectDestination.set(ii, new DBDateTime(longValue));
            }
        }
        dBDateTimeObjectDestination.setSize(longChunk.size());
    }
}