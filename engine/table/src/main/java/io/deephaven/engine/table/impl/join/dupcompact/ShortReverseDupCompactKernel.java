/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupCompactKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public class ShortReverseDupCompactKernel implements DupCompactKernel {

    static final ShortReverseDupCompactKernel INSTANCE = new ShortReverseDupCompactKernel();

    private ShortReverseDupCompactKernel() {
        // Use the singleton INSTANCE
    }

    @Override
    public int compactDuplicates(
            @NotNull final WritableChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> rowKeys) {
        return compactDuplicates(chunkToCompact.asWritableShortChunk(), rowKeys);
    }

    @Override
    public int compactDuplicatesPreferFirst(
            @NotNull final WritableChunk<? extends Any> chunkToCompact,
            @NotNull final WritableIntChunk<ChunkPositions> chunkPositions) {
        return compactDuplicatesPreferFirst(chunkToCompact.asWritableShortChunk(), chunkPositions);
    }

    private static int compactDuplicates(
            @NotNull final WritableShortChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> rowKeys) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        short last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final short current = chunkToCompact.get(rpos);
            if (!leq(last, current)) {
                return rpos;
            }
            last = current;

            while (rpos < inputSize - 1 && eq(current, chunkToCompact.get(rpos + 1))) {
                rpos++;
            }
            chunkToCompact.set(wpos, current);
            rowKeys.set(wpos, rowKeys.get(rpos));
            rpos++;
            wpos++;
        }
        chunkToCompact.setSize(wpos);
        rowKeys.setSize(wpos);

        return -1;
    }

    private static int compactDuplicatesPreferFirst(
            @NotNull final WritableShortChunk<? extends Any> chunkToCompact,
            @NotNull final WritableIntChunk<ChunkPositions> chunkPositions) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        short last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final short current = chunkToCompact.get(rpos);
            if (!leq(last, current)) {
                return rpos;
            }
            last = current;

            chunkToCompact.set(wpos, current);
            chunkPositions.set(wpos, chunkPositions.get(rpos));
            rpos++;
            wpos++;

            while (rpos < inputSize && eq(current, chunkToCompact.get(rpos))) {
                rpos++;
            }
        }
        chunkToCompact.setSize(wpos);
        chunkPositions.setSize(wpos);

        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(short lhs, short rhs) {
        return -1 * Short.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(short lhs, short rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(short lhs, short rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
