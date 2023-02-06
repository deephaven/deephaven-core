/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharVectorColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.vector.LongVector;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UngroupedLongVectorColumnSource extends UngroupedColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {
    private ColumnSource<LongVector> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedLongVectorColumnSource(ColumnSource<LongVector> innerSource) {
        super(Long.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Long get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        final Long result;
        if (isUngroupable) {
            result = (Long)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final LongVector segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_LONG : segmentArray.get(offset);
        }
        return (result == NULL_LONG ? null : result);
    }


    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }

        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedLong(segment, offset);
        }

        final LongVector segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_LONG : segmentArray.get(offset);
    }


    @Override
    public Long getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        final Long result;
        if (isUngroupable) {
            result = (Long)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final LongVector segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_LONG : segmentArray.get(offset);
        }

        return (result == NULL_LONG ? null : result);
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevLong(segment, offset);
        }

        final LongVector segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_LONG : segmentArray.get(offset);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
