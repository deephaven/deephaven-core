/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.dbarrays.*;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class UngroupedByteDbArrayColumnSource extends UngroupedColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte {
    private ColumnSource<DbByteArray> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedByteDbArrayColumnSource(ColumnSource<DbByteArray> innerSource) {
        super(Byte.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Byte get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        final Byte result;
        if (isUngroupable) {
            result = (Byte)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final DbByteArray segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
        }
        return (result == NULL_BYTE ? null : result);
    }


    @Override
    public byte getByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }

        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedByte(segment, offset);
        }

        final DbByteArray segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
    }


    @Override
    public Byte getPrev(long index) {
        if (index < 0) {
            return null;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        final Byte result;
        if (isUngroupable) {
            result = (Byte)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final DbByteArray segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
        }

        return (result == NULL_BYTE ? null : result);
    }

    @Override
    public byte getPrevByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevByte(segment, offset);
        }

        final DbByteArray segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }
}
