//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of row keys that grows by inserting disjoint row sets in any key order, and that can remove the keys it holds
 * from another row set between insertions.
 *
 * <p>
 * Inserting keys before the end of a sparse row set copies its storage, so inserting many small row sets into a single
 * row set in an order unrelated to their keys takes quadratic time. {@link RowSetUnionBatcher} avoids that by deferring
 * the merge, but holds no set of keys that can be consulted before {@link RowSetUnionBatcher#build() build}. This
 * accumulator instead holds its keys in disjoint row sets, each more than twice the size of the next, so there are at
 * most {@code log2(size()) + 1} of them to consult.
 *
 * <p>
 * An inserted row set that lies entirely past the end of a held row set is spliced onto the largest such row set, which
 * costs what is spliced. Any other inserted row set is held on its own as the smallest. Either way, a held row set that
 * is no longer more than twice the size of the one after it is merged with it, as a binary counter carries, so each key
 * is copied a logarithmic number of times.
 *
 * <p>
 * {@link #build()} hands the union to the caller. Everything this accumulator still holds at {@link #close()} is
 * closed.
 */
public final class RowSetAccumulator implements SafeCloseable {

    /** Disjoint row sets, each more than twice the size of the one after it. */
    private final List<WritableRowSet> rowSets = new ArrayList<>();

    /** The number of keys in all of {@link #rowSets}. */
    private long size;

    /**
     * @return The number of row keys inserted since this accumulator was constructed or last built
     */
    public long size() {
        return size;
    }

    /**
     * Remove every row key of this accumulator from {@code rows}, modifying {@code rows} at most once.
     *
     * @param rows The row set to remove this accumulator's row keys from
     */
    public void removeFrom(@NotNull final WritableRowSet rows) {
        RowSetBuilderRandom heldRowsBuilder = null;
        for (final RowSet rowSet : rowSets) {
            if (rows.overlaps(rowSet)) {
                if (heldRowsBuilder == null) {
                    heldRowsBuilder = RowSetFactory.builderRandom();
                }
                try (final RowSet heldRows = rows.intersect(rowSet)) {
                    heldRowsBuilder.addRowSet(heldRows);
                }
            }
        }
        if (heldRowsBuilder != null) {
            try (final RowSet heldRows = heldRowsBuilder.build()) {
                rows.remove(heldRows);
            }
        }
    }

    /**
     * Insert the row keys of {@code rows}, which must not contain any row key of this accumulator.
     *
     * @param rows The row keys to insert; not retained, and not modified
     */
    public void insert(@NotNull final RowSet rows) {
        if (rows.isEmpty()) {
            return;
        }
        size += rows.size();
        int grownIndex = appendTarget(rows.firstRowKey());
        if (grownIndex >= 0) {
            rowSets.get(grownIndex).insert(rows);
        } else {
            rowSets.add(rows.copy());
            grownIndex = rowSets.size() - 1;
        }
        carry(grownIndex);
    }

    /**
     * Remove and return the union of the row keys inserted, leaving this accumulator empty.
     *
     * @return A row set of every row key inserted since this accumulator was constructed or last built, which the
     *         caller owns
     */
    public WritableRowSet build() {
        if (rowSets.isEmpty()) {
            return RowSetFactory.empty();
        }
        final WritableRowSet result = rowSets.get(0);
        for (int ii = 1; ii < rowSets.size(); ++ii) {
            try (final WritableRowSet rowSet = rowSets.get(ii)) {
                result.subsume(rowSet);
            }
        }
        rowSets.clear();
        size = 0;
        return result;
    }

    /**
     * Close everything not yet handed over by {@link #build()}.
     */
    @Override
    public void close() {
        try {
            SafeCloseable.closeAll(rowSets);
        } finally {
            rowSets.clear();
            size = 0;
        }
    }

    /**
     * @return The index of the largest held row set whose last row key is before {@code firstRowKey}, or {@code -1} if
     *         there is none
     */
    private int appendTarget(final long firstRowKey) {
        for (int ii = 0; ii < rowSets.size(); ++ii) {
            if (rowSets.get(ii).lastRowKey() < firstRowKey) {
                return ii;
            }
        }
        return -1;
    }

    /**
     * Restore the size invariant after the row set at {@code grownIndex} grew. It is still more than twice the size of
     * the row set after it, but the row sets before it may no longer be more than twice its size.
     */
    private void carry(int grownIndex) {
        while (grownIndex > 0) {
            final WritableRowSet previous = rowSets.get(grownIndex - 1);
            final WritableRowSet grown = rowSets.get(grownIndex);
            if (previous.size() > 2 * grown.size()) {
                return;
            }
            try (grown) {
                previous.subsume(grown);
            }
            rowSets.remove(grownIndex--);
        }
    }

    /**
     * @return The size of each held row set, largest first
     */
    @VisibleForTesting
    long[] rowSetSizes() {
        return rowSets.stream().mapToLong(RowSet::size).toArray();
    }
}
