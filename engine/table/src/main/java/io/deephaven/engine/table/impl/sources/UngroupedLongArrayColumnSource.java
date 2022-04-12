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

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UngroupedLongArrayColumnSource extends UngroupedColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {
    private ColumnSource<long[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedLongArrayColumnSource(ColumnSource<long[]> innerSource) {
        super(Long.class);
        this.innerSource = innerSource;
    }

    @Override
    public long getLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        long[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_LONG;
        }
        return array[offset];
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        long[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_LONG;
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
