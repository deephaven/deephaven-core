package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.LongChunk;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RowSet} that may be mutated (that is, have its contents changed in-place). Note that all {@link RowSet}
 * implementations conform to this interface, but many APIs only expose the super-interface to discourage inappropriate
 * changes.
 */
public interface WritableRowSet extends RowSet {

    /**
     * Add a single key to this RowSet if it's not already present.
     *
     * @param key The key to add
     */
    void insert(long key);

    /**
     * Add all keys in a closed range to this RowSet if they are not already present.
     *
     * @param startKey The first key to add
     * @param endKey The last key to add (inclusive)
     */
    void insertRange(long startKey, long endKey);

    /**
     * Add all of the (ordered) keys in a slice of {@code keys} to this RowSet if they are not already present.
     *
     * @param keys The {@link LongChunk} of {@link OrderedRowKeys} to insert
     * @param offset The offset in {@code keys} to begin inserting keys from
     * @param length The number of keys to insert
     */
    void insert(LongChunk<OrderedRowKeys> keys, int offset, int length);

    /**
     * Add all of the keys in {@code added} to this RowSet if they are not already present.
     *
     * @param added The RowSet to add
     */
    void insert(RowSet added);

    /**
     * Remove a single key from this RowSet if it's present.
     *
     * @param key The key to remove
     */
    void remove(long key);

    /**
     * Remove all keys in a closed range from this RowSet if they are present.
     *
     * @param startKey The first key to remove
     * @param endKey The last key to remove (inclusive)
     */
    void removeRange(long startKey, long endKey);

    /**
     * Remove all of the (ordered) keys in a slice of {@code keys} from this RowSet if they are present.
     *
     * @param keys The {@link LongChunk} of {@link OrderedRowKeys} to remove
     * @param offset The offset in {@code keys} to begin removing keys from
     * @param length The number of keys to remove
     */
    void remove(LongChunk<OrderedRowKeys> keys, int offset, int length);

    /**
     * Remove all of the keys in {@code removed} that are present in this RowSet.
     *
     * @param removed The RowSet to remove
     */
    void remove(RowSet removed);

    /**
     * Simultaneously adds the keys from the first RowSet and removes the keys from the second one. API assumption: the
     * intersection of added and removed is empty.
     */
    void update(RowSet added, RowSet removed);

    /**
     * Removes all the keys from <i>other</i> RowSet that are present in this RowSet.
     *
     * @return a new RowSet representing the keys removed
     */
    @NotNull
    default WritableRowSet extract(@NotNull final RowSet other) {
        final WritableRowSet ret = this.intersect(other);
        remove(ret);
        return ret;
    }

    /**
     * Modifies the RowSet by removing any keys not in the rowSetToIntersect argument.
     *
     * @param rowSetToIntersect a rowSet with the keys to retain; any other keys not in rowSetToIntersect will be
     *        removed.
     */
    void retain(RowSet rowSetToIntersect);

    /**
     * Modifies the RowSet by keeping only keys in the interval [startRowKey, endRowKey]
     *
     * @param startRowKey beginning of interval of keys to keep.
     * @param endRowKey endRowKey of interval of keys to keep (inclusive).
     */
    void retainRange(long startRowKey, long endRowKey);

    void clear();

    void shiftInPlace(long shiftAmount);

    /**
     * For each key in the provided RowSet, shift it by shiftAmount and insert it in the current RowSet.
     *
     * @param shiftAmount the amount to add to each key in the RowSet argument before insertion.
     * @param other the RowSet with the keys to shift and insert.
     */
    void insertWithShift(long shiftAmount, RowSet other);

    /**
     * May reclaim some unused memory.
     */
    void compact();

    @Override
    default TrackingWritableRowSet trackingCast() {
        return (TrackingWritableRowSet) this;
    }

    /**
     * <p>
     * Destructively convert this WritableRowSet into a {@link TrackingWritableRowSet}.
     * <p>
     * This is really only suitable when the caller "owns" this WritableRowSet. Programming errors may occur if the any
     * code holds onto references to {@code this} rather than the result, because there may be ambiguity about resource
     * ownership.
     * <p>
     * Implementations are free to transfer ownership of resources from this object to the result. As such, it is an
     * error to directly use this object afterwards; callers must instead use the returned result.
     * <p>
     * It is an error to invoke this on an instance that is already tracking.
     *
     * @return A {@link TrackingWritableRowSet} constructed from this WritableRowSet, or {@code this} if already
     *         tracking
     */
    TrackingWritableRowSet toTracking();
}
