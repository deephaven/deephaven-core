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

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * An Ungrouped Column sourced for the Boxed Type Integer.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedIntArrayColumnSource extends UngroupedColumnSource<Integer>
        implements MutableColumnSourceGetDefaults.ForObject<Integer> {
    private ColumnSource<Integer[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedIntArrayColumnSource(ColumnSource<Integer[]> innerSource) {
        super(Integer.class);
        this.innerSource = innerSource;
    }

    @Override
    public Integer get(long rowKey) {
        final int result = getInt(rowKey);
        return (result == NULL_INT ? null : result);
    }


    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        long segment = rowKey >> base;
        int offset = (int) (rowKey & ((1 << base) - 1));
        Integer[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_INT;
        }
        return array[offset];
    }


    @Override
    public Integer getPrev(long rowKey) {
        final int result = getPrevInt(rowKey);
        return (result == NULL_INT ? null : result);
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1 << getPrevBase()) - 1));
        Integer[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_INT;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
