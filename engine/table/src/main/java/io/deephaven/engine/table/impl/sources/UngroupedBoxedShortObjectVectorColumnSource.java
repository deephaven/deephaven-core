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
    public short getShort(long rowKey) {
        final Short result = get(rowKey);
        return result == null ? NULL_SHORT : result;
    }

    @Override
    public short getPrevShort(long rowKey) {
        final Short result = getPrev(rowKey);
        return result == null ? NULL_SHORT : result;
    }
}
