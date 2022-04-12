/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedCharArrayColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class UngroupedDoubleArrayColumnSource extends UngroupedColumnSource<Double> implements MutableColumnSourceGetDefaults.ForDouble {
    private ColumnSource<double[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedDoubleArrayColumnSource(ColumnSource<double[]> innerSource) {
        super(Double.class);
        this.innerSource = innerSource;
    }

    @Override
    public double getDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        double[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_DOUBLE;
        }
        return array[offset];
    }

    @Override
    public double getPrevDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        double[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_DOUBLE;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
    
    @Override
    public boolean preventsParallelism() {
        return innerSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
