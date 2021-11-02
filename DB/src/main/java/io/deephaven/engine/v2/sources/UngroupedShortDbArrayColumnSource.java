/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.dbarrays.*;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class UngroupedShortDbArrayColumnSource extends UngroupedColumnSource<Short> implements MutableColumnSourceGetDefaults.ForShort {
    private ColumnSource<DbShortArray> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedShortDbArrayColumnSource(ColumnSource<DbShortArray> innerSource) {
        super(Short.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Short get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        final Short result;
        if (isUngroupable) {
            result = (Short)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final DbShortArray segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_SHORT : segmentArray.get(offset);
        }
        return (result == NULL_SHORT ? null : result);
    }


    @Override
    public short getShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }

        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedShort(segment, offset);
        }

        final DbShortArray segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_SHORT : segmentArray.get(offset);
    }


    @Override
    public Short getPrev(long index) {
        if (index < 0) {
            return null;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        final Short result;
        if (isUngroupable) {
            result = (Short)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final DbShortArray segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_SHORT : segmentArray.get(offset);
        }

        return (result == NULL_SHORT ? null : result);
    }

    @Override
    public short getPrevShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevShort(segment, offset);
        }

        final DbShortArray segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_SHORT : segmentArray.get(offset);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }
}
