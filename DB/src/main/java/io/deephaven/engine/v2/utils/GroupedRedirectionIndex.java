package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.ResettableWritableLongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * The GroupedRedirectionIndex is intended for situations where you have several Indices that represent contiguous rows
 * of your output table and a flat output rowSet.
 *
 * When sorting a table by it's grouping column, instead of using a large contiguous RedirectionIndex, we simply store
 * the indices for each group and the accumulated cardinality. We then binary search in the accumulated cardinality for
 * a given key; and fetch the corresponding offset from that group's TrackingMutableRowSet.
 *
 * This RedirectionIndex does not support mutation.
 */
public class GroupedRedirectionIndex implements RedirectionIndex {
    /**
     * The total size of the redirection TrackingMutableRowSet.
     */
    private final long size;
    /**
     * The accumulated size of each group. Element 0 is the size of the first group. Element 1 is the size of the first
     * and second group. The final element will equal size.
     */
    private final long[] groupSizes;
    /**
     * The actual TrackingMutableRowSet for each group; parallel with groupSizes.
     */
    private final RowSet[] groups;

    /**
     * If you are doing repeated get calls, then we must redo the binary search from scratch each time. To avoid this
     * behavior, we cache the last slot that you found, so that repeated calls to get() skip the binary search if the
     * key is within the same group as your last call.
     */
    private final ThreadLocal<SavedContext> threadContext = ThreadLocal.withInitial(SavedContext::new);

    public GroupedRedirectionIndex(long size, long[] groupSizes, RowSet[] groups) {
        this.size = size;
        this.groupSizes = groupSizes;
        this.groups = groups;
    }

    @Override
    public long get(long key) {
        if (key < 0 || key >= size) {
            return RowSet.NULL_ROW_KEY;
        }

        int slot;

        final SavedContext savedContext = threadContext.get();
        if (savedContext.firstKey <= key && savedContext.lastKey >= key) {
            slot = savedContext.lastSlot;
        } else {
            // figure out which group we belong to
            slot = Arrays.binarySearch(groupSizes, key);
            if (slot < 0) {
                slot = ~slot;
            } else {
                // we are actually in the slot after this one
                slot += 1;
            }
            savedContext.lastSlot = slot;
            if (slot == 0) {
                savedContext.firstKey = 0;
            } else {
                savedContext.firstKey = groupSizes[slot - 1];
            }
            savedContext.lastKey = groupSizes[slot];
        }

        if (slot == 0) {
            return groups[slot].get(key);
        } else {
            return groups[slot].get(key - groupSizes[slot - 1]);
        }
    }

    @Override
    public long getPrev(long key) {
        return get(key);
    }

    private static class SavedContext {
        SavedContext() {
            firstKey = lastKey = -1;
        }

        long firstKey;
        long lastKey;
        int lastSlot;
    }

    @Override
    public void fillChunk(@NotNull FillContext fillContext,
            @NotNull WritableLongChunk<Attributes.RowKeys> mappedKeysOut, @NotNull RowSequence keysToMap) {
        final MutableInt outputPosition = new MutableInt(0);
        final MutableInt lastSlot = new MutableInt(0);
        mappedKeysOut.setSize(keysToMap.intSize());
        try (final ResettableWritableLongChunk<Attributes.RowKeys> resettableKeys =
                ResettableWritableLongChunk.makeResettableChunk()) {
            keysToMap.forAllLongRanges((begin, end) -> {
                while (begin <= end) {
                    // figure out which group we belong to, based on the first key in the range
                    int slot = Arrays.binarySearch(groupSizes, lastSlot.intValue(), groupSizes.length, begin);
                    if (slot < 0) {
                        slot = ~slot;
                    } else {
                        // we are actually in the slot after this one
                        slot += 1;
                    }
                    // for the next one we should not search the beginning of the array
                    lastSlot.setValue(slot);

                    // for the first key, we have an offset of 0; for other keys we need to offset the key
                    final long beginKeyWithOffset = slot == 0 ? begin : begin - groupSizes[slot - 1];

                    final long size = end - begin + 1;
                    final int groupSize;

                    final WritableLongChunk<Attributes.RowKeys> chunkToFill = resettableKeys.resetFromTypedChunk(
                            mappedKeysOut, outputPosition.intValue(), mappedKeysOut.size() - outputPosition.intValue());
                    if (beginKeyWithOffset > 0 || (beginKeyWithOffset + size < groups[slot].size())) {
                        try (RowSequence rowSequenceByPosition =
                                groups[slot].getRowSequenceByPosition(beginKeyWithOffset, size)) {
                            rowSequenceByPosition.fillRowKeyChunk(chunkToFill);
                            groupSize = rowSequenceByPosition.intSize();
                        }
                    } else {
                        groups[slot].fillRowKeyChunk(chunkToFill);
                        groupSize = groups[slot].intSize();
                    }
                    outputPosition.add(groupSize);
                    begin += groupSize;

                }
            });
        }
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext fillContext,
            @NotNull WritableLongChunk<Attributes.RowKeys> mappedKeysOut, @NotNull RowSequence keysToMap) {
        fillChunk(fillContext, mappedKeysOut, keysToMap);
    }

    @Override
    public long remove(long leftIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long put(long key, long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startTrackingPrevValues() {
        // nothing to do, we are explicitly immutable
    }
}
