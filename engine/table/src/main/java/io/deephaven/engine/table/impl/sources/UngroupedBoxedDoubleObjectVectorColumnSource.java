//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit UngroupedBoxedCharObjectVectorColumnSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
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
    public double getDouble(long rowKey) {
        final Double result = get(rowKey);
        return result == null ? NULL_DOUBLE : result;
    }

    @Override
    public double getPrevDouble(long rowKey) {
        final Double result = getPrev(rowKey);
        return result == null ? NULL_DOUBLE : result;
    }
}
