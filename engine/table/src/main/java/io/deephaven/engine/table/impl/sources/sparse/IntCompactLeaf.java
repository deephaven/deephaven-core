//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharCompactLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

/**
 * A leaf of a {@link io.deephaven.engine.table.impl.sources.IntegerAdaptiveSparseArraySource} that holds fewer values
 * than a full block. Values are addressed by their offset within the block, from 0 to
 * {@link SparseConstants#BLOCK_SIZE} - 1; an offset without a value reads as null.
 * <p>
 * The offsets a leaf holds never change. Setting the value of an offset the leaf holds writes it in place; adding or
 * removing offsets makes a new leaf, which the caller puts in place of this one. A reader of this leaf therefore finds
 * each value at the correct offset, however the leaf is concurrently written.
 */
public abstract sealed class IntCompactLeaf permits IntSortedLeaf, IntBitmapLeaf {

    /**
     * @return the number of offsets that hold a value
     */
    public abstract int size();

    /**
     * @return whether {@code offset} holds a value
     */
    public abstract boolean contains(int offset);

    /**
     * @return the value at {@code offset}, or null if it holds none
     */
    public abstract int get(int offset);

    /**
     * Set the value at {@code offset}, adding the offset if it holds no value.
     *
     * @param sortedThreshold the most values a sorted leaf holds; a sorted leaf that would hold more makes a bitmap
     *        leaf instead
     * @return this leaf, if it holds {@code offset}; otherwise a new leaf holding its values and the new one
     */
    public abstract IntCompactLeaf put(int offset, int value, int sortedThreshold);

    /**
     * Set the values at several offsets, adding the offsets that hold no value. When every offset already holds a
     * value, only the values of those offsets are written, so writers of distinct offsets may call this concurrently.
     *
     * @param offsets the offsets, ascending and distinct, in the first {@code count} entries
     * @param src the values, in the order of {@code offsets}
     * @param srcOffset the position in {@code src} of the value for {@code offsets[0]}
     * @param count the number of offsets
     * @param sortedThreshold the most values a sorted leaf holds; a sorted leaf that would hold more makes a bitmap
     *        leaf instead
     * @return this leaf, if it holds every offset; otherwise a new leaf holding its values and the new ones
     */
    public abstract IntCompactLeaf putSorted(int[] offsets, IntChunk<?> src, int srcOffset, int count,
            int sortedThreshold);

    /**
     * Add each of {@code offsets[0, count)}, ascending and distinct, that holds no value, with a null value.
     *
     * @param sortedThreshold the most values a sorted leaf holds; a sorted leaf that would hold more makes a bitmap
     *        leaf instead
     * @return this leaf, if it holds every offset; otherwise a new leaf holding its values and the added offsets
     */
    public abstract IntCompactLeaf insertAbsent(int[] offsets, int count, int sortedThreshold);

    /**
     * @return the number of {@code offsets[0, count)}, ascending and distinct, that hold no value
     */
    public abstract int countAbsent(int[] offsets, int count);

    /**
     * Remove the value at {@code offset}, if it holds one.
     *
     * @return this leaf, if it does not hold {@code offset}; otherwise a new leaf holding its other values
     */
    public abstract IntCompactLeaf remove(int offset);

    /**
     * Remove the values at offsets {@code firstOffset} through {@code lastOffset} inclusive.
     *
     * @return this leaf, if it holds none of those offsets; otherwise a new leaf holding its other values
     */
    public abstract IntCompactLeaf removeRange(int firstOffset, int lastOffset);

    /**
     * Copy every value into {@code block} at its offset, leaving the other entries of {@code block} unchanged.
     */
    public abstract void copyTo(int[] block);

    /**
     * Write the offsets from {@code firstOffset} through {@code lastOffset} inclusive that hold a value into
     * {@code dest}, in ascending order.
     *
     * @return the number of offsets written
     */
    public abstract int copyOffsets(int firstOffset, int lastOffset, int[] dest);

    /**
     * Write the value of each offset from {@code firstOffset} through {@code lastOffset} inclusive that holds a value
     * into {@code dest} at {@code destOffset + offset - firstOffset}, leaving the other entries of {@code dest}
     * unchanged.
     */
    public abstract void overlayRange(int firstOffset, int lastOffset, WritableIntChunk<?> dest, int destOffset);

    /**
     * Fill {@code dest}, starting at {@code destOffset}, with the values for offsets {@code firstOffset} through
     * {@code lastOffset} inclusive.
     */
    public abstract void fillRange(int firstOffset, int lastOffset, WritableIntChunk<?> dest, int destOffset);

    /**
     * Fill {@code dest}, from {@code destOffset}, with the values for the offsets of {@code keys[start, end)}, row keys
     * within this leaf in ascending order; an offset without a value reads as null. The keys are visited in one pass
     * over the leaf.
     */
    public abstract void fillKeys(LongChunk<? extends RowKeys> keys, int start, int end, WritableIntChunk<?> dest,
            int destOffset);

    /**
     * Write the value for the offset of each of {@code keys[start, end)}, row keys within this leaf in ascending order,
     * that holds a value into {@code dest}, from {@code destOffset}, leaving the other entries of {@code dest}
     * unchanged.
     */
    public abstract void overlayKeys(LongChunk<? extends RowKeys> keys, int start, int end, WritableIntChunk<?> dest,
            int destOffset);

    /**
     * @return an estimate of the bytes this leaf occupies
     */
    public abstract long estimateSize();
}
