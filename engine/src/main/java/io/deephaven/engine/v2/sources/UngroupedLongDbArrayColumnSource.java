/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.structures.vector.*;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UngroupedLongDbArrayColumnSource extends UngroupedColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {
    private ColumnSource<DbLongArray> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedLongDbArrayColumnSource(ColumnSource<DbLongArray> innerSource) {
        super(Long.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Long get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        final Long result;
        if (isUngroupable) {
            result = (Long)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final DbLongArray segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_LONG : segmentArray.get(offset);
        }
        return (result == NULL_LONG ? null : result);
    }


    @Override
    public long getLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }

        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedLong(segment, offset);
        }

        final DbLongArray segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_LONG : segmentArray.get(offset);
    }


    @Override
    public Long getPrev(long index) {
        if (index < 0) {
            return null;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        final Long result;
        if (isUngroupable) {
            result = (Long)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final DbLongArray segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_LONG : segmentArray.getPrev(offset);
        }

        return (result == NULL_LONG ? null : result);
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevLong(segment, offset);
        }

        final DbLongArray segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_LONG : segmentArray.getPrev(offset);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }
}
