//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Accumulates the union of row sets handed over one at a time, merging them a batch at a time rather than inserting
 * each one into a growing result.
 *
 * <p>
 * Inserting each row set into the result separately costs a pass over the result every time, which is quadratic when
 * the inputs are disjoint and arrive in an order unrelated to their keys; a batch costs one
 * {@link RowSetFactory#union(Collection) union}, which sorts itself first. The batch size is what bounds that: a caller
 * passes how many row sets it expects to produce, and one no larger than {@link #MAX_BATCH_SIZE} merges the whole input
 * at once.
 *
 * <p>
 * Merging every batch into one result would reintroduce the same problem one level up, at one pass per batch rather
 * than one per row set. Instead the entries are held in two regions of a list of {@code 2 * batchSize} slots. A full
 * batch collapses into a single row set that stays where it is, so the front of the list fills with collapsed groups
 * while the back gathers the next batch. The groups are allowed to fill their half of the list; the batch that would
 * need a slot past it folds everything into one instead. A result is therefore merged into again once per
 * {@code batchSize} batches instead of once per batch, which is the same tree the multi-pass merge inside {@code union}
 * builds, one level up.
 *
 * <p>
 * Two things are handled here rather than at the merge. {@link RowSet#isEmpty() Empty} row sets are dropped as they
 * arrive, so a batch never spends a slot on one. A row set that only appends past the end of the last entry is spliced
 * onto it in place instead of taking a slot of its own, which the row set implementations satisfy without merging range
 * by range; input that arrives in ascending order therefore collapses into a single row set that {@link #build()} hands
 * over as it stands, with no union at all.
 *
 * <p>
 * {@link #build()} is what releases the result to the caller. Everything this batcher is still holding at
 * {@link #close()} is closed, so a caller drives it from a try-with-resources block and a traversal that throws part
 * way through abandons what it had gathered rather than handing back half a union.
 */
public final class RowSetUnionBatcher implements SafeCloseable {

    /**
     * The most row sets that will be gathered into one batch, however many the caller asks for. Large enough that the
     * merge amortizes the pass it costs, small enough that input driven by data rather than by the shape of the query
     * cannot make this hold an unbounded number of row sets. This caps the batch rather than every merge:
     * {@link #build()} hands {@code union} the collapsed groups as well as the batch, at most {@code 2 * batchSize - 1}
     * row sets.
     */
    public static final int MAX_BATCH_SIZE = 1024;

    private final int batchSize;

    /**
     * The collapsed groups, then the batch being gathered. Never longer than twice the batch size: the groups are
     * folded into one as soon as they would take more than half of it.
     */
    private final List<WritableRowSet> entries;

    /** How many leading {@link #entries} are collapsed groups rather than part of the batch being gathered. */
    private int groupCount;

    /**
     * The last entry of {@link #entries}, which a row set that appends to it is spliced onto. Null whenever there are
     * no entries.
     */
    private WritableRowSet run;

    /**
     * @param batchSize The number of row sets to gather before merging, which a caller passes as the number it expects
     *        to produce. Clamped to {@code [1, }{@link #MAX_BATCH_SIZE}{@code ]}: under the cap the whole input merges
     *        at once, and over it, or where the count is only an upper bound or no bound at all, the cap takes over and
     *        the count costs nothing to have passed. Taken as a {@code long} so that a caller counting rows rather than
     *        objects has nothing to narrow and no reason to know the cap.
     */
    public RowSetUnionBatcher(final long batchSize) {
        this.batchSize = (int) Math.min(Math.max(1L, batchSize), MAX_BATCH_SIZE);
        // Bounded by the clamp above, so this is the list's greatest extent and not just a starting point.
        entries = new ArrayList<>(2 * this.batchSize);
    }

    /**
     * Add {@code rowSet} to the union, merging the gathered batch if it is now full.
     *
     * <p>
     * Ownership of {@code rowSet} passes here: it may be closed before this call returns, and the caller must not use
     * or close it afterwards. A caller that keeps its row set hands over a {@link RowSet#copy() copy}, which is a
     * copy-on-write reference that later mutation of the original does not disturb.
     *
     * @param rowSet The row set to add; ownership passes here
     */
    public void add(@NotNull final WritableRowSet rowSet) {
        if (rowSet.isEmpty()) {
            rowSet.close();
            return;
        }
        if (appendsToRun(rowSet)) {
            try (rowSet) {
                run.insert(rowSet);
            }
            return;
        }
        startRun(rowSet);
    }

    /**
     * Merge whatever is outstanding and hand over the union of everything added since this batcher was constructed or
     * last built. Ownership passes to the caller, and this batcher is left empty and ready for more.
     *
     * @return A {@link WritableRowSet} containing every row key added, which the caller owns. Not necessarily a newly
     *         constructed one: a single outstanding row set is handed back as it stands.
     */
    public WritableRowSet build() {
        run = null;
        groupCount = 0;
        if (entries.isEmpty()) {
            return RowSetFactory.empty();
        }
        return mergeFrom(0);
    }

    /**
     * Whether inserting {@code rowSet} into the run only extends it past its last row key, which the row set
     * implementations satisfy by splicing rather than by merging range by range.
     */
    private boolean appendsToRun(final RowSet rowSet) {
        return run != null && rowSet.firstRowKey() > run.lastRowKey();
    }

    private void startRun(final WritableRowSet rowSet) {
        entries.add(rowSet);
        run = rowSet;
        if (entries.size() - groupCount < batchSize) {
            return;
        }
        // The batch is full. Collapse it in place, and fold the groups together if that was the last free slot.
        run = null;
        final WritableRowSet group = mergeFrom(groupCount);
        entries.add(group);
        if (++groupCount > batchSize) {
            final WritableRowSet folded = mergeFrom(0);
            entries.add(folded);
            groupCount = 1;
            run = folded;
            return;
        }
        run = group;
    }

    /**
     * Remove {@code entries[from, size)} and return their union, which the caller owns. A single entry is handed back
     * as it stands rather than merged with itself.
     */
    private WritableRowSet mergeFrom(final int from) {
        // A view of the entries being merged. Clearing it is what removes them from the list behind it.
        final List<WritableRowSet> tail = entries.subList(from, entries.size());
        if (tail.size() == 1) {
            return tail.remove(0);
        }
        try {
            // The union borrows what it is handed, so these entries are still ours to close once it has read them.
            return RowSetFactory.union(tail);
        } finally {
            SafeCloseable.closeAll(tail.iterator());
            tail.clear();
        }
    }

    /**
     * The number of row sets in the batch being gathered, which is what the next merge will be handed. A run of appends
     * does not grow it, and collapsing it empties it.
     */
    @VisibleForTesting
    int pendingBatchSize() {
        return entries.size() - groupCount;
    }

    /**
     * The number of collapsed groups waiting to be folded together.
     */
    @VisibleForTesting
    int groupCount() {
        return groupCount;
    }

    /**
     * Close everything not yet handed over by {@link #build()}.
     */
    @Override
    public void close() {
        run = null;
        groupCount = 0;
        try {
            SafeCloseable.closeAll(entries.iterator());
        } finally {
            entries.clear();
        }
    }
}
