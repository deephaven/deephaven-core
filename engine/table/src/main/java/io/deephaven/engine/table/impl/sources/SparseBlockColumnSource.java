//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK0_SHIFT;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK1_SHIFT;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.LOG_BLOCK_SIZE;

/**
 * A column source that stores its values in the blocks of a sparse page table, as laid out by
 * {@link io.deephaven.engine.table.impl.sources.sparse.SparseConstants}, and can release the blocks, block2 and block1
 * structures that hold no live rows.
 */
public interface SparseBlockColumnSource {

    /**
     * At the end of this cycle, release the given blocks, block2 structures and block1 structures.
     *
     * @param blocksToClear the indices ({@code rowKey >> LOG_BLOCK_SIZE}) of the lowest level blocks to release
     * @param removeBlocks2 the indices ({@code rowKey >> BLOCK1_SHIFT}) of the block2 structures to release from their
     *        block1 structures
     * @param removeBlocks1 the indices ({@code rowKey >> BLOCK0_SHIFT}) of the block1 structures to release from the
     *        block0 structure
     * @param empty whether the result holds no rows after this cycle
     */
    void clearBlocks(RowSet blocksToClear, RowSet removeBlocks2, RowSet removeBlocks1, boolean empty);

    /**
     * At the end of this cycle, release each block, block2 and block1 structure of {@code sources} that holds a row key
     * of {@code candidateRowKeys} and no row key of {@code liveRowSet}.
     *
     * <p>
     * The values each source holds at row keys outside {@code liveRowSet} must be unused after this cycle. The cost is
     * proportional to the number of blocks that {@code candidateRowKeys} spans, so it should hold the row keys that
     * were live at the start of this cycle and are not live now (the removed rows and the rows that shifts vacated),
     * and as few others as practical. Each source may be passed to this method, or to
     * {@link #clearBlocks(RowSet, RowSet, RowSet, boolean)}, at most once per cycle.
     * </p>
     *
     * @param candidateRowKeys the row keys whose blocks may have no live rows
     * @param liveRowSet the row keys whose values the sources hold after this cycle
     * @param sources the sources to release blocks from
     */
    static void clearBlocksWithoutLiveRows(
            @NotNull final RowSet candidateRowKeys,
            @NotNull final RowSet liveRowSet,
            @NotNull final SparseBlockColumnSource... sources) {
        if (sources.length == 0 || candidateRowKeys.isEmpty()) {
            return;
        }
        try (final RowSet removeBlocks = getBlocksWithoutLiveRows(candidateRowKeys, liveRowSet, LOG_BLOCK_SIZE);
                final RowSet removeBlocks2 = removeBlocks.isNonempty()
                        ? getBlocksWithoutLiveRows(candidateRowKeys, liveRowSet, BLOCK1_SHIFT)
                        : RowSetFactory.empty();
                final RowSet removeBlocks1 = removeBlocks2.isNonempty()
                        ? getBlocksWithoutLiveRows(candidateRowKeys, liveRowSet, BLOCK0_SHIFT)
                        : RowSetFactory.empty()) {
            if (removeBlocks.isEmpty()) {
                return;
            }
            for (final SparseBlockColumnSource source : sources) {
                source.clearBlocks(removeBlocks, removeBlocks2, removeBlocks1, liveRowSet.isEmpty());
            }
        }
    }

    /**
     * At the end of this cycle, release each block, block2 and block1 structure of {@code source} that holds a row key
     * vacated this cycle and no row key of {@code outerRowSet}. The vacated row keys are those removed and those within
     * the range of a shift, as {@link #clearBlocksWithoutLiveRows(RowSet, RowSet, SparseBlockColumnSource[])} requires;
     * the shifted row keys that are still live keep their blocks.
     *
     * @param removed the row keys removed from {@code outerRowSet} this cycle, in the pre-shift key space
     * @param shifted the shifts applied to {@code outerRowSet} this cycle
     * @param outerRowSet the row keys whose values {@code source} holds, whose previous value is the row set at the
     *        start of this cycle
     * @param source the source to release blocks from
     */
    static void clearVacatedBlocks(
            @NotNull final RowSet removed,
            @NotNull final RowSetShiftData shifted,
            @NotNull final TrackingRowSet outerRowSet,
            @NotNull final SparseBlockColumnSource source) {
        if (shifted.empty()) {
            clearBlocksWithoutLiveRows(removed, outerRowSet, source);
            return;
        }
        final RowSet prevRowSet = outerRowSet.prev();
        try (final WritableRowSet candidates = removed.copy()) {
            final int shiftCount = shifted.size();
            for (int ii = 0; ii < shiftCount; ++ii) {
                try (final RowSet shiftedRows =
                        prevRowSet.subSetByKeyRange(shifted.getBeginRange(ii), shifted.getEndRange(ii))) {
                    candidates.insert(shiftedRows);
                }
            }
            clearBlocksWithoutLiveRows(candidates, outerRowSet, source);
        }
    }

    /**
     * @return the indices ({@code rowKey >> logBlockSize}) of the blocks of {@code 1 << logBlockSize} row keys that
     *         hold a row key of {@code candidateRowKeys} and no row key of {@code liveRowSet}
     */
    @NotNull
    private static RowSet getBlocksWithoutLiveRows(
            final RowSet candidateRowKeys,
            final RowSet liveRowSet,
            final int logBlockSize) {
        final long blockSize = 1L << logBlockSize;
        final RowSet.SearchIterator liveIterator = liveRowSet.searchIterator();
        final RowSet.SearchIterator candidateIterator = candidateRowKeys.searchIterator();

        final RowSetBuilderSequential removeBlockBuilder = RowSetFactory.builderSequential();
        long startOfNextBlock = 0;
        while (candidateIterator.advance(startOfNextBlock)) {
            final long candidateKey = candidateIterator.currentValue();
            final long endOfCandidateBlock = candidateKey | (blockSize - 1);

            final long startOfCandidateBlock = candidateKey & ~(blockSize - 1);
            if (!liveIterator.advance(startOfCandidateBlock) || liveIterator.currentValue() > endOfCandidateBlock) {
                removeBlockBuilder.appendKey(candidateKey >> logBlockSize);
            }
            if (endOfCandidateBlock == Long.MAX_VALUE) {
                break;
            }
            startOfNextBlock = endOfCandidateBlock + 1;
        }
        return removeBlockBuilder.build();
    }
}
