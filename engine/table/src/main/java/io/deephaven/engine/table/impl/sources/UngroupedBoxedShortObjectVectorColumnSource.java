/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * An Ungrouped Column sourced for the Boxed Type Short.
 * <p>
 * The UngroupedBoxedC-harVectorColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
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
