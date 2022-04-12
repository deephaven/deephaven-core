/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * An Ungrouped Column sourced for the Boxed Type Float.
 * <p>
 * The UngroupedBoxedC-harVectorColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedFloatObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Float> {

    public UngroupedBoxedFloatObjectVectorColumnSource(ColumnSource<ObjectVector<Float>> innerSource) {
        super(innerSource);
    }

    @Override
    public float getFloat(long index) {
        final Float result = get(index);
        return result == null ? NULL_FLOAT : result;
    }

    @Override
    public float getPrevFloat(long index) {
        final Float result = getPrev(index);
        return result == null ? NULL_FLOAT : result;
    }
}
