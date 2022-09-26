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
import io.deephaven.vector.DoubleVector;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class UngroupedDoubleVectorColumnSource extends UngroupedColumnSource<Double> implements MutableColumnSourceGetDefaults.ForDouble {
    private ColumnSource<DoubleVector> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedDoubleVectorColumnSource(ColumnSource<DoubleVector> innerSource) {
        super(Double.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Double get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        final Double result;
        if (isUngroupable) {
            result = (Double)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final DoubleVector segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_DOUBLE : segmentArray.get(offset);
        }
        return (result == NULL_DOUBLE ? null : result);
    }


    @Override
    public double getDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }

        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedDouble(segment, offset);
        }

        final DoubleVector segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_DOUBLE : segmentArray.get(offset);
    }


    @Override
    public Double getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        final Double result;
        if (isUngroupable) {
            result = (Double)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final DoubleVector segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_DOUBLE : segmentArray.get(offset);
        }

        return (result == NULL_DOUBLE ? null : result);
    }

    @Override
    public double getPrevDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevDouble(segment, offset);
        }

        final DoubleVector segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_DOUBLE : segmentArray.get(offset);
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
