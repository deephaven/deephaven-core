/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.dbarrays.DbArray;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * An Ungrouped Column sourced for the Boxed Type Float.
 * <p>
 * The UngroupedBoxedC-harDbArrayColumnSource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedFloatDbArrayColumnSource extends UngroupedDbArrayColumnSource<Float> {

    public UngroupedBoxedFloatDbArrayColumnSource(ColumnSource<DbArray<Float>> innerSource) {
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
