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
import io.deephaven.vector.ByteVector;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class UngroupedByteVectorColumnSource extends UngroupedColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte {
    private ColumnSource<ByteVector> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedByteVectorColumnSource(ColumnSource<ByteVector> innerSource) {
        super(Byte.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Byte get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        final Byte result;
        if (isUngroupable) {
            result = (Byte)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final ByteVector segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
        }
        return (result == NULL_BYTE ? null : result);
    }


    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }

        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedByte(segment, offset);
        }

        final ByteVector segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
    }


    @Override
    public Byte getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        final Byte result;
        if (isUngroupable) {
            result = (Byte)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final ByteVector segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
        }

        return (result == NULL_BYTE ? null : result);
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevByte(segment, offset);
        }

        final ByteVector segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_BYTE : segmentArray.get(offset);
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
