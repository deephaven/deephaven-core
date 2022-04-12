/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An Ungrouped Column sourced for the Boxed Type Long.
 * <p>
 * The UngroupedBoxedC-harVectorColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedLongObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Long> {

    public UngroupedBoxedLongObjectVectorColumnSource(ColumnSource<ObjectVector<Long>> innerSource) {
        super(innerSource);
    }

    @Override
    public long getLong(long index) {
        final Long result = get(index);
        return result == null ? NULL_LONG : result;
    }

    @Override
    public long getPrevLong(long index) {
        final Long result = getPrev(index);
        return result == null ? NULL_LONG : result;
    }
}
