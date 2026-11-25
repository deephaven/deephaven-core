//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit UngroupedCharArrayColumnSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class UngroupedShortArrayColumnSource extends UngroupedColumnSource<Short>
        implements MutableColumnSourceGetDefaults.ForShort {
    private ColumnSource<short[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedShortArrayColumnSource(ColumnSource<short[]> innerSource) {
        super(Short.class);
        this.innerSource = innerSource;
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        long segment = rowKey >> base;
        long offset = rowKey & ((1L << base) - 1);
        short[] array = innerSource.get(segment);
        if (array == null || offset >= array.length) {
            return NULL_SHORT;
        }
        return array[(int) offset];
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        long segment = rowKey >> getPrevBase();
        long offset = rowKey & ((1L << getPrevBase()) - 1);
        short[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length) {
            return NULL_SHORT;
        }
        return array[(int) offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
