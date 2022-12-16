/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSet;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * The GroupedWritableRowRedirection is intended for situations where you have several row sets that represent
 * contiguous rows of your output table and a flat output RowSet.
 * <p>
 * When sorting a table by its grouping column, instead of using a large contiguous WritableRowRedirection, we simply
 * store the row sets for each group and the accumulated cardinality. We then binary search in the accumulated
 * cardinality for a given key; and fetch the corresponding offset from that group's row set.
 * <p>
 * This WritableRowRedirection does not support mutation.
 */
public class GroupedWritableRowRedirection implements WritableRowRedirection {
    /**
     * The total size of the row redirection.
     */
    private final long size;
    /**
     * The accumulated size of each group. Element 0 is the size of the first group. Element 1 is the size of the first
     * and second group. The final element will equal size.
     */
    private final long[] groupSizes;
    /**
     * The actual RowSet for each group; parallel with groupSizes.
     */
    private final RowSet[] groups;

    /**
     * If you are doing repeated get calls, then we must redo the binary search from scratch each time. To avoid this
     * behavior, we cache the last slot that you found, so that repeated calls to get() skip the binary search if the
     * key is within the same group as your last call.
     */
    private final ThreadLocal<SavedContext> threadContext = ThreadLocal.withInitial(SavedContext::new);

    public GroupedWritableRowRedirection(long size, long[] groupSizes, RowSet[] groups) {
        this.size = size;
        this.groupSizes = groupSizes;
        this.groups = groups;
    }

    @Override
    public long get(long outerRowKey) {
        if (outerRowKey < 0 || outerRowKey >= size) {
            return RowSequence.NULL_ROW_KEY;
        }

        int slot;

        final SavedContext savedContext = threadContext.get();
        if (savedContext.firstKey <= outerRowKey && savedContext.lastKey >= outerRowKey) {
            slot = savedContext.lastSlot;
        } else {
            // figure out which group we belong to
            slot = Arrays.binarySearch(groupSizes, outerRowKey);
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
            return groups[slot].get(outerRowKey);
        } else {
            return groups[slot].get(outerRowKey - groupSizes[slot - 1]);
        }
    }

    @Override
    public long getPrev(long outerRowKey) {
        return get(outerRowKey);
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
    public void fillChunk(
            @NotNull FillContext fillContext,
            @NotNull WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull RowSequence outerRowKeys) {
        final MutableInt outputPosition = new MutableInt(0);
        final MutableInt lastSlot = new MutableInt(0);
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(outerRowKeys.intSize());
        try (final ResettableWritableLongChunk<Any> resettableKeys =
                ResettableWritableLongChunk.makeResettableChunk()) {
            outerRowKeys.forAllRowKeyRanges((begin, end) -> {
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

                    final WritableLongChunk<? super RowKeys> chunkToFill = resettableKeys.resetFromTypedChunk(
                            innerRowKeysTyped,
                            outputPosition.intValue(),
                            innerRowKeysTyped.size() - outputPosition.intValue());
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
            @NotNull WritableChunk<? super RowKeys> innerRowKeys, @NotNull RowSequence outerRowKeys) {
        fillChunk(fillContext, innerRowKeys, outerRowKeys);
    }

    @Override
    public long remove(long outerRowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long put(long outerRowKey, long innerRowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startTrackingPrevValues() {
        // nothing to do, we are explicitly immutable
    }
}
