/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class UngroupedCharArrayColumnSource extends UngroupedColumnSource<Character> implements MutableColumnSourceGetDefaults.ForChar {
    private ColumnSource<char[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedCharArrayColumnSource(ColumnSource<char[]> innerSource) {
        super(Character.class);
        this.innerSource = innerSource;
    }

    @Override
    public char getChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        char[] array = innerSource.get(segment);
        if(array == null || offset >= array.length) {
            return NULL_CHAR;
        }
        return array[offset];
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        char[] array = innerSource.getPrev(segment);
        if(array == null || offset >= array.length) {
            return NULL_CHAR;
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
