/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharArrayColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * An Ungrouped Column sourced for the Boxed Type Float.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedFloatArrayColumnSource extends UngroupedColumnSource<Float> implements MutableColumnSourceGetDefaults.ForObject<Float> {
    private ColumnSource<Float[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedFloatArrayColumnSource(ColumnSource<Float[]> innerSource) {
        super(Float.class);
        this.innerSource = innerSource;
    }

    @Override
    public Float get(long index) {
        final float result = getFloat(index);
        return (result == NULL_FLOAT?null:result);
    }


    @Override
    public float getFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        Float[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_FLOAT;
        }
        return array[offset];
    }


    @Override
    public Float getPrev(long index) {
        final float result = getPrevFloat(index);
        return (result == NULL_FLOAT?null:result);
    }

    @Override
    public float getPrevFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        Float[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_FLOAT;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
