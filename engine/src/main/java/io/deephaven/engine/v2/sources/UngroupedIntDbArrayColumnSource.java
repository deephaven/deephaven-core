/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.structures.vector.*;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class UngroupedIntDbArrayColumnSource extends UngroupedColumnSource<Integer> implements MutableColumnSourceGetDefaults.ForInt {
    private ColumnSource<DbIntArray> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedIntDbArrayColumnSource(ColumnSource<DbIntArray> innerSource) {
        super(Integer.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Integer get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        final Integer result;
        if (isUngroupable) {
            result = (Integer)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final DbIntArray segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_INT : segmentArray.get(offset);
        }
        return (result == NULL_INT ? null : result);
    }


    @Override
    public int getInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }

        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedInt(segment, offset);
        }

        final DbIntArray segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_INT : segmentArray.get(offset);
    }


    @Override
    public Integer getPrev(long index) {
        if (index < 0) {
            return null;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        final Integer result;
        if (isUngroupable) {
            result = (Integer)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final DbIntArray segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_INT : segmentArray.getPrev(offset);
        }

        return (result == NULL_INT ? null : result);
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevInt(segment, offset);
        }

        final DbIntArray segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_INT : segmentArray.getPrev(offset);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }
}
