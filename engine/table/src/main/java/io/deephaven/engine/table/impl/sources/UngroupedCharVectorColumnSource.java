/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.vector.CharVector;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class UngroupedCharVectorColumnSource extends UngroupedColumnSource<Character> implements MutableColumnSourceGetDefaults.ForChar {
    private ColumnSource<CharVector> innerSource;
    private final boolean isUngroupable;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedCharVectorColumnSource(ColumnSource<CharVector> innerSource) {
        super(Character.class);
        this.innerSource = innerSource;
        this.isUngroupable = innerSource instanceof UngroupableColumnSource && ((UngroupableColumnSource)innerSource).isUngroupable();
    }

    @Override
    public Character get(long index) {
        if (index < 0) {
            return null;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        final Character result;
        if (isUngroupable) {
            result = (Character)((UngroupableColumnSource)innerSource).getUngrouped(segment, offset);
            if (result == null)
                return null;
        } else {
            final CharVector segmentArray = innerSource.get(segment);
            result = segmentArray == null ? NULL_CHAR : segmentArray.get(offset);
        }
        return (result == NULL_CHAR ? null : result);
    }


    @Override
    public char getChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }

        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedChar(segment, offset);
        }

        final CharVector segmentArray = innerSource.get(segment);
        return segmentArray == null ? NULL_CHAR : segmentArray.get(offset);
    }


    @Override
    public Character getPrev(long index) {
        if (index < 0) {
            return null;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        final Character result;
        if (isUngroupable) {
            result = (Character)((UngroupableColumnSource)innerSource).getUngroupedPrev(segment, offset);
            if (result == null) {
                return null;
            }
        } else {
            final CharVector segmentArray = innerSource.getPrev(segment);
            result = segmentArray == null ? NULL_CHAR : segmentArray.get(offset);
        }

        return (result == NULL_CHAR ? null : result);
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }

        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));

        if (isUngroupable) {
            return ((UngroupableColumnSource)innerSource).getUngroupedPrevChar(segment, offset);
        }

        final CharVector segmentArray = innerSource.getPrev(segment);
        return segmentArray == null ? NULL_CHAR : segmentArray.get(offset);
    }

    @Override
    public boolean isImmutable() {
        return false;
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
