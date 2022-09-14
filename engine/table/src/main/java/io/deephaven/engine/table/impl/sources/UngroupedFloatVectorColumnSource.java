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
import io.deephaven.vector.FloatVector;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class UngroupedFloatVectorColumnSource extends UngroupedColumnSource<Float> implements MutableColumnSourceGetDefaults.ForFloat {
    private ColumnSource<FloatVector> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedFloatVectorColumnSource(ColumnSource<FloatVector> innerSource) {
        super(Float.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Float get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        final Float result;
        if (isUngroupable) {
            result = (Float)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final FloatVector segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_FLOAT : segmentArray.get(offset);
        }
        return (result == NULL_FLOAT ? null : result);
    }


    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }

        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedFloat(segment, offset);
        }

        final FloatVector segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_FLOAT : segmentArray.get(offset);
    }


    @Override
    public Float getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        final Float result;
        if (isUngroupable) {
            result = (Float)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final FloatVector segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_FLOAT : segmentArray.get(offset);
        }

        return (result == NULL_FLOAT ? null : result);
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevFloat(segment, offset);
        }

        final FloatVector segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_FLOAT : segmentArray.get(offset);
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
