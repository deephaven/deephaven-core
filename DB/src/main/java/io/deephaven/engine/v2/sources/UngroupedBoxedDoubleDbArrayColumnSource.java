/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharDbArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.dbarrays.DbArray;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * An Ungrouped Column sourced for the Boxed Type Double.
 * <p>
 * The UngroupedBoxedC-harDbArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedDoubleDbArrayColumnSource extends UngroupedDbArrayColumnSource<Double> {

    public UngroupedBoxedDoubleDbArrayColumnSource(ColumnSource<DbArray<Double>> innerSource) {
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
