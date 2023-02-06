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
import io.deephaven.vector.IntVector;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class UngroupedIntVectorColumnSource extends UngroupedColumnSource<Integer> implements MutableColumnSourceGetDefaults.ForInt {
    private ColumnSource<IntVector> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedIntVectorColumnSource(ColumnSource<IntVector> innerSource) {
        super(Integer.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Integer get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        final Integer result;
        if (isUngroupable) {
            result = (Integer)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final IntVector segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_INT : segmentArray.get(offset);
        }
        return (result == NULL_INT ? null : result);
    }


    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }

        long segment = rowKey >>base;
        int offset = (int) (rowKey & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedInt(segment, offset);
        }

        final IntVector segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_INT : segmentArray.get(offset);
    }


    @Override
    public Integer getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));
        final Integer result;
        if (isUngroupable) {
            result = (Integer)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final IntVector segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_INT : segmentArray.get(offset);
        }

        return (result == NULL_INT ? null : result);
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }

        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevInt(segment, offset);
        }

        final IntVector segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_INT : segmentArray.get(offset);
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
