/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharArrayColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An Ungrouped Column sourced for the Boxed Type Long.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedLongArrayColumnSource extends UngroupedColumnSource<Long> implements MutableColumnSourceGetDefaults.ForObject<Long> {
    private ColumnSource<Long[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedLongArrayColumnSource(ColumnSource<Long[]> innerSource) {
        super(Long.class);
        this.innerSource = innerSource;
    }

    @Override
    public Long get(long index) {
        final long result = getLong(index);
        return (result == NULL_LONG?null:result);
    }


    @Override
    public long getLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        Long[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_LONG;
        }
        return array[offset];
    }


    @Override
    public Long getPrev(long index) {
        final long result = getPrevLong(index);
        return (result == NULL_LONG?null:result);
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        Long[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_LONG;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
