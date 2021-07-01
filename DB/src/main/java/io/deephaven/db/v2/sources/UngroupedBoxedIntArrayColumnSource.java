/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * An Ungrouped Column sourced for the Boxed Type Integer.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedIntArrayColumnSource extends UngroupedColumnSource<Integer> implements MutableColumnSourceGetDefaults.ForObject<Integer> {
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
    public Integer get(long index) {
        final int result = getInt(index);
        return (result == NULL_INT?null:result);
    }


    @Override
    public int getInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        Integer[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_INT;
        }
        return array[offset];
    }


    @Override
    public Integer getPrev(long index) {
        final int result = getPrevInt(index);
        return (result == NULL_INT?null:result);
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
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
