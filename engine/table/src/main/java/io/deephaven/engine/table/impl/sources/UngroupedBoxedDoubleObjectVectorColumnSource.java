/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * An Ungrouped Column sourced for the Boxed Type Double.
 * <p>
 * The UngroupedBoxedC-harVectorColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedDoubleObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Double> {

    public UngroupedBoxedDoubleObjectVectorColumnSource(ColumnSource<ObjectVector<Double>> innerSource) {
        super(innerSource);
    }

    @Override
    public double getDouble(long index) {
        final Double result = get(index);
        return result == null ? NULL_DOUBLE : result;
    }

    @Override
    public double getPrevDouble(long index) {
        final Double result = getPrev(index);
        return result == null ? NULL_DOUBLE : result;
    }
}
