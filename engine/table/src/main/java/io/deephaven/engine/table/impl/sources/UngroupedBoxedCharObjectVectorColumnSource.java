/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * An Ungrouped Column sourced for the Boxed Type Character.
 * <p>
 * The UngroupedBoxedC-harVectorColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedCharObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Character> {

    public UngroupedBoxedCharObjectVectorColumnSource(ColumnSource<ObjectVector<Character>> innerSource) {
        super(innerSource);
    }

    @Override
    public char getChar(long rowKey) {
        final Character result = get(rowKey);
        return result == null ? NULL_CHAR : result;
    }

    @Override
    public char getPrevChar(long rowKey) {
        final Character result = getPrev(rowKey);
        return result == null ? NULL_CHAR : result;
    }
}
