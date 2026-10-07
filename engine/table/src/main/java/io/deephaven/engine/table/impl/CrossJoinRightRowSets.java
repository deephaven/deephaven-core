//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;

/**
 * The rows of a static right table, grouped by the id that a {@link KeyIdHasher} gives their key.
 * <p>
 * The rows are {@link #add added} one chunk at a time, in row key order, and then {@link #build built} into a row set
 * for each id, once. The row sets can be read only after they are built.
 */
final class CrossJoinRightRowSets {
    // each id's RowSetBuilderSequential until build, and its TrackingRowSet afterwards; null for an id with no rows
    private final ObjectArraySource<Object> groups = new ObjectArraySource<>(Object.class);
    private boolean built = false;
    private long maxGroupSize = 0;

    /**
     * Add each row to the group for its key's id, skipping the rows whose id is {@link KeyIdHasher#NULL_ID}.
     *
     * @param rows the rows, following every row added before
     * @param ids the id of each row's key
     * @param idCapacity the hasher's {@link KeyIdHasher#idCapacity() id capacity}
     */
    void add(final RowSequence rows, final IntChunk<Values> ids, final int idCapacity) {
        Assert.eqFalse(built, "built");
        groups.ensureCapacity(idCapacity);
        final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
        for (int ii = 0; ii < rowKeys.size(); ++ii) {
            final int id = ids.get(ii);
            if (id == KeyIdHasher.NULL_ID) {
                continue;
            }
            RowSetBuilderSequential builder = (RowSetBuilderSequential) groups.getUnsafe(id);
            if (builder == null) {
                groups.set(id, builder = RowSetFactory.builderSequential());
            }
            builder.appendKey(rowKeys.get(ii));
        }
    }

    /**
     * Build the row set of each id from the rows added. This may be called only once, after which no rows may be added.
     *
     * @param idCapacity the hasher's {@link KeyIdHasher#idCapacity() id capacity}
     */
    void build(final int idCapacity) {
        Assert.eqFalse(built, "built");
        built = true;
        groups.ensureCapacity(idCapacity);
        for (int id = 0; id < idCapacity; ++id) {
            final RowSetBuilderSequential builder = (RowSetBuilderSequential) groups.getUnsafe(id);
            if (builder == null) {
                continue;
            }
            final TrackingWritableRowSet rowSet = builder.build().toTracking();
            groups.set(id, rowSet);
            maxGroupSize = Math.max(maxGroupSize, rowSet.size());
        }
    }

    /**
     * @param id the id of a key
     * @return the right rows with the key, or null if there are none
     */
    TrackingRowSet get(final long id) {
        Assert.eqTrue(built, "built");
        return (TrackingRowSet) groups.getUnsafe(id);
    }

    /**
     * @return the size of the largest row set
     */
    long maxGroupSize() {
        Assert.eqTrue(built, "built");
        return maxGroupSize;
    }
}
