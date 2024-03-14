//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit UngroupedBoxedCharArrayColumnSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * An Ungrouped Column sourced for the Boxed Type Short.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedShortArrayColumnSource extends UngroupedColumnSource<Short>
        implements MutableColumnSourceGetDefaults.ForObject<Short> {
    private ColumnSource<Short[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedShortArrayColumnSource(ColumnSource<Short[]> innerSource) {
        super(Short.class);
        this.innerSource = innerSource;
    }

    @Override
    public Short get(long rowKey) {
        final short result = getShort(rowKey);
        return (result == NULL_SHORT ? null : result);
    }


    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        long segment = rowKey >> base;
        int offset = (int) (rowKey & ((1 << base) - 1));
        Short[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_SHORT;
        }
        return array[offset];
    }


    @Override
    public Short getPrev(long rowKey) {
        final short result = getPrevShort(rowKey);
        return (result == NULL_SHORT ? null : result);
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1 << getPrevBase()) - 1));
        Short[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_SHORT;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
