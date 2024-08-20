//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.Vector;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link ColumnSource} that provides {@link Vector vectors} of type T, backed by a same typed
 * {@link SegmentedSortedMultiSet}.
 *
 * @param <K> The SSM Type
 * @param <T> The provided Array type
 */
public interface SsmBackedColumnSource<K extends SegmentedSortedMultiSet, T extends Vector>
        extends ColumnSource<T> {

    ObjectArraySource<K> getUnderlyingSource();

    /**
     * Get the current SSM at the specified key. This does not permute it in any way.
     *
     * @param key the key to get the ssm for.
     * @return the SSM
     */
    K getCurrentSsm(long key);

    /**
     * Get the ssm at the specified key, creating one if none existed. This method will update the current previous
     * tracking state of the SSM.
     *
     * @param key the key to get the ssm for.
     * @return the SSM at the key, or a new one.
     */
    K getOrCreate(long key);

    /**
     * Set the SSM at the specified key to null
     *
     * @param key the key to get the ssm for.
     */
    void clear(long key);

    /**
     * Ensure the source has at least `capacity` capacity
     * 
     * @param capacity the capacity to ensure.
     */
    void ensureCapacity(long capacity);

    /**
     * Clear out any tracked deltas from recent computations.
     *
     * @param indices the set of indices to clear deltas for.
     */
    void clearDeltas(RowSet indices);
}
