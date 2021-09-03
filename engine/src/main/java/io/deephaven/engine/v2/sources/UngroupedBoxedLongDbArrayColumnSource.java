/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.structures.vector.DbArray;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An Ungrouped Column sourced for the Boxed Type Long.
 * <p>
 * The UngroupedBoxedC-harDbArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedLongDbArrayColumnSource extends UngroupedDbArrayColumnSource<Long> {

    public UngroupedBoxedLongDbArrayColumnSource(ColumnSource<DbArray<Long>> innerSource) {
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
