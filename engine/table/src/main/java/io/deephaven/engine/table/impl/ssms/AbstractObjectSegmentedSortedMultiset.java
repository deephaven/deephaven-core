//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.vector.ObjectVector;

/**
 * The Object segmented sorted multisets. {@link ObjectSegmentedSortedMultiset} tests equality with
 * {@link io.deephaven.util.compare.ObjectComparisons#compareEquals(Object, Object)}, which is correct for any
 * Comparable; {@link EqualsConsistentObjectSegmentedSortedMultiset} tests equality with {@code equals}, which is
 * correct for data types whose natural ordering is consistent with equals (see
 * {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}).
 *
 * <p>
 * Aggregation operators hold their sets as this type. An aggregation reads the registry once when it creates an
 * operator and passes the decision to the factory or source that creates the operator's sets and to its compact
 * kernels, and the per-bucket paths call these methods without casts.
 */
public abstract class AbstractObjectSegmentedSortedMultiset
        implements SegmentedSortedMultiSet<Object>, ObjectVector<Object> {

    /**
     * Insert the {@code length} values beginning at {@code offset}. The values must be sorted and hold one value for
     * each class of equal values.
     *
     * @param valuesToInsert the values to insert
     * @param counts the number of times each value occurs
     * @param offset the first position in valuesToInsert and counts to insert
     * @param length the number of positions in valuesToInsert and counts to insert
     * @return true if any new values were inserted
     */
    public abstract boolean insert(WritableObjectChunk<Object, ? extends Values> valuesToInsert,
            WritableIntChunk<ChunkLengths> counts, int offset, int length);

    /**
     * Insert {@code count} copies of a single {@code value}.
     *
     * @param value the value to insert
     * @param count the number of copies to insert
     * @return true if the value was not already present
     */
    public abstract boolean insert(Object value, long count);

    /**
     * Remove the {@code length} values beginning at {@code offset}, which must be sorted and currently present.
     *
     * @param removeContext the removal context
     * @param valuesToRemove the values to remove
     * @param counts the number of times each value is removed
     * @param offset the first position in valuesToRemove and counts to remove
     * @param length the number of positions in valuesToRemove and counts to remove
     * @return true if any value was fully removed
     */
    public abstract boolean remove(RemoveContext removeContext,
            WritableObjectChunk<Object, ? extends Values> valuesToRemove, WritableIntChunk<ChunkLengths> counts,
            int offset, int length);

    /**
     * Remove {@code count} copies of a single {@code value}, which must currently be present.
     *
     * @param value the value to remove
     * @param count the number of copies to remove
     * @return true if the value was fully removed
     */
    public abstract boolean remove(Object value, long count);

    /**
     * @return the minimum value of this set
     */
    public abstract Object getMinObject();

    /**
     * @return the maximum value of this set
     */
    public abstract Object getMaxObject();

    /**
     * Copy the values removed since the deltas were cleared into {@code chunk}, beginning at {@code position}.
     *
     * @param chunk the destination chunk
     * @param position the first position of chunk to write
     */
    public abstract void fillRemovedChunk(WritableObjectChunk<Object, ? extends Values> chunk, int position);

    /**
     * Copy the values added since the deltas were cleared into {@code chunk}, beginning at {@code position}.
     *
     * @param chunk the destination chunk
     * @param position the first position of chunk to write
     */
    public abstract void fillAddedChunk(WritableObjectChunk<Object, ? extends Values> chunk, int position);

    /**
     * @return the values of this set as they were when the deltas were last cleared
     */
    public abstract ObjectVector getPrevValues();
}
