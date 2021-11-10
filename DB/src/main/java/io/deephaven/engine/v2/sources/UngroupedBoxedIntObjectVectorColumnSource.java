/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * An Ungrouped Column sourced for the Boxed Type Integer.
 * <p>
 * The UngroupedBoxedC-harDbArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedIntObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Integer> {

    public UngroupedBoxedIntObjectVectorColumnSource(ColumnSource<ObjectVector<Integer>> innerSource) {
        super(innerSource);
    }

    @Override
    public int getInt(long index) {
        final Integer result = get(index);
        return result == null ? NULL_INT : result;
    }

    @Override
    public int getPrevInt(long index) {
        final Integer result = getPrev(index);
        return result == null ? NULL_INT : result;
    }
}
