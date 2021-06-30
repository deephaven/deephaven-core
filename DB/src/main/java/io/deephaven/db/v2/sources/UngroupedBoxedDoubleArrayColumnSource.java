/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * An Ungrouped Column sourced for the Boxed Type Double.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedDoubleArrayColumnSource extends UngroupedColumnSource<Double> implements MutableColumnSourceGetDefaults.ForObject<Double> {
    private ColumnSource<Double[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedDoubleArrayColumnSource(ColumnSource<Double[]> innerSource) {
        super(Double.class);
        this.innerSource = innerSource;
    }

    @Override
    public Double get(long index) {
        final double result = getDouble(index);
        return (result == NULL_DOUBLE?null:result);
    }


    @Override
    public double getDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        Double[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_DOUBLE;
        }
        return array[offset];
    }


    @Override
    public Double getPrev(long index) {
        final double result = getPrevDouble(index);
        return (result == NULL_DOUBLE?null:result);
    }

    @Override
    public double getPrevDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        Double[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_DOUBLE;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
