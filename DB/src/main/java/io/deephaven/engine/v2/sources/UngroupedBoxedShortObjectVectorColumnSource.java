/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.dbarrays.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * An Ungrouped Column sourced for the Boxed Type Short.
 * <p>
 * The UngroupedBoxedC-harDbArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedShortObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Short> {

    public UngroupedBoxedShortObjectVectorColumnSource(ColumnSource<ObjectVector<Short>> innerSource) {
        super(innerSource);
    }

    @Override
    public short getShort(long index) {
        final Short result = get(index);
        return result == null ? NULL_SHORT : result;
    }

    @Override
    public short getPrevShort(long index) {
        final Short result = getPrev(index);
        return result == null ? NULL_SHORT : result;
    }
}
