//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharCompactLeaf and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;

/**
 * A leaf of a {@link io.deephaven.engine.table.impl.sources.IntegerAdaptiveSparseArraySource} that holds fewer values
 * than a full block. Values are addressed by their offset within the block, from 0 to
 * {@link SparseConstants#BLOCK_SIZE} - 1; an offset without a value reads as null.
 */
public abstract class IntCompactLeaf {

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
     */
    public abstract void put(int offset, int value);

    /**
     * Set the values at several offsets, adding the offsets that hold no value.
     *
     * @param offsets the offsets, ascending and distinct, in the first {@code count} entries
     * @param src the values, in the order of {@code offsets}
     * @param srcOffset the position in {@code src} of the value for {@code offsets[0]}
     * @param count the number of offsets
     */
    public abstract void putSorted(int[] offsets, IntChunk<?> src, int srcOffset, int count);

    /**
     * @return the number of {@code offsets[0, count)}, ascending and distinct, that hold no value
     */
    public int countAbsent(final int[] offsets, final int count) {
        int absent = 0;
        for (int ii = 0; ii < count; ++ii) {
            if (!contains(offsets[ii])) {
                ++absent;
            }
        }
        return absent;
    }

    /**
     * Remove the value at {@code offset}, if it holds one.
     */
    public abstract void remove(int offset);

    /**
     * Remove the values at offsets {@code firstOffset} through {@code lastOffset} inclusive.
     */
    public void removeRange(final int firstOffset, final int lastOffset) {
        for (int offset = firstOffset; offset <= lastOffset; ++offset) {
            remove(offset);
        }
    }

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
     * @return an estimate of the bytes this leaf occupies
     */
    public abstract long estimateSize();
}
