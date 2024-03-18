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
    public long getLong(long rowKey) {
        final Long result = get(rowKey);
        return result == null ? NULL_LONG : result;
    }

    @Override
    public long getPrevLong(long rowKey) {
        final Long result = getPrev(rowKey);
        return result == null ? NULL_LONG : result;
    }
}
