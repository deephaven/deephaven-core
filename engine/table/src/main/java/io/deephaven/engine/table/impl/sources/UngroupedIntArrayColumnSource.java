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

import static io.deephaven.util.QueryConstants.NULL_INT;

public class UngroupedIntArrayColumnSource extends UngroupedColumnSource<Integer> implements MutableColumnSourceGetDefaults.ForInt {
    private ColumnSource<int[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedIntArrayColumnSource(ColumnSource<int[]> innerSource) {
        super(Integer.class);
        this.innerSource = innerSource;
    }

    @Override
    public int getInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        int[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_INT;
        }
        return array[offset];
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        int[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_INT;
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
