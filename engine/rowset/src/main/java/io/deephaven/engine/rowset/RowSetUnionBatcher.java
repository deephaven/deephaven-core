//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
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
 * passes how many row sets it expects to produce, and one no larger than {@link #maxBatchSize} merges the whole input
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
 * arrive, so a batch never spends a slot on one. A row set that falls clear of the last entry is folded into it in
 * place instead of taking a slot of its own: past the end always, since the row set implementations splice rather than
 * merge range by range, and below the start when the entry it would be folded into is no larger than it is. Input that
 * arrives in ascending order therefore collapses into a single row set that {@link #build()} hands over as it stands,
 * with no union at all, as does a descending stream of row sets each at least as large as what it has already gathered.
 *
 * <p>
 * {@link #build()} is what releases the result to the caller. Everything this batcher is still holding at
 * {@link #close()} is closed, so a caller drives it from a try-with-resources block and a traversal that throws part
 * way through abandons what it had gathered rather than handing back half a union.
 */
public final class RowSetUnionBatcher implements SafeCloseable {

    /** Default for {@link #maxBatchSize}. */
    public static final int DEFAULT_MAX_BATCH_SIZE = 8192;

    /**
     * The most row sets gathered into one batch, whatever count a caller asks for: large enough that each merge
     * amortizes the pass it costs, small enough that input driven by data rather than by the shape of the query cannot
     * make this hold an unbounded number of row sets. This caps the batch rather than every merge: {@link #build()}
     * hands {@code union} the collapsed groups as well as the batch, at most {@code 2 * batchSize - 1} row sets.
     *
     * <p>
     * Read from the {@code RowSetUnionBatcher.maxBatchSize} configuration property, default
     * {@link #DEFAULT_MAX_BATCH_SIZE}. The union builds a batch of small row sets in one linear pass, so a larger cap
     * hands it more at once and leaves fewer batch results to merge afterwards; the cap is what bounds how many row
     * sets are held while the batch is gathered.
     */
    @VisibleForTesting
    public static int maxBatchSize = Configuration.getInstance().getIntegerForClassWithDefault(
            RowSetUnionBatcher.class, "maxBatchSize", DEFAULT_MAX_BATCH_SIZE);

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
     *        to produce. Clamped to {@code [1, }{@link #maxBatchSize}{@code ]}: under the cap the whole input merges at
     *        once, and over it, or where the count is only an upper bound or no bound at all, the cap takes over and
     *        the count costs nothing to have passed. Taken as a {@code long} so that a caller counting rows rather than
     *        objects has nothing to narrow and no reason to know the cap. The cap is taken as configured.
     * @throws IllegalArgumentException If the resulting batch size is not positive, or twice it would not fit an array,
     *         since the entries list must be able to hold the batch and the collapsed groups together
     */
    public RowSetUnionBatcher(final long batchSize) {
        this.batchSize = (int) Math.min(Math.max(1L, batchSize), maxBatchSize);
        if (this.batchSize <= 0 || this.batchSize > ArrayUtil.MAX_ARRAY_SIZE / 2) {
            throw new IllegalArgumentException("batch size " + this.batchSize + " from configured maxBatchSize "
                    + maxBatchSize + " must be in [1, " + ArrayUtil.MAX_ARRAY_SIZE / 2 + "]");
        }
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
        if (joinsRun(rowSet)) {
            // Ownership passed to us, so this may reuse either side's storage for the result.
            try (rowSet) {
                run.subsume(rowSet);
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
     * Whether {@code rowSet} lies clear of the run on one side or the other, and is worth folding into it rather than
     * taking a slot of its own.
     *
     * <p>
     * Above the run is always worth it: the row set implementations splice onto the end, so it costs what is spliced
     * and nothing per entry already there. Below the run is not the mirror image, and not because the direction is
     * chosen badly -- {@link WritableRowSet#subsume subsume} picks the cheaper of the two and picks it well. It is that
     * both directions are proportional to the run rather than to what arrived: opening room at the front moves the run,
     * and appending the run onto what arrived copies it. Keys are held in order, so something has to move either way.
     *
     * <p>
     * What that leaves the batcher is not which direction but whether to fold at all, since it is the batcher that
     * decides how often the cost is paid. Over {@link RspBitmap} pairs a 64 entry run takes a 256K entry row set in
     * under a microsecond, where a 256K entry run takes a 64 entry row set in half a millisecond; paying the second of
     * those once per row set is quadratic over a descending stream. So a prepend is folded only when the run is no
     * larger than what is being added, which pays for the move with the entries it arrives with. Anything else starts a
     * run of its own and reaches the batch merge, which sorts before it merges and does not care what order the input
     * came in.
     */
    private boolean joinsRun(final RowSet rowSet) {
        if (run == null) {
            return false;
        }
        if (rowSet.firstRowKey() > run.lastRowKey()) {
            return true;
        }
        return rowSet.lastRowKey() < run.firstRowKey() && entryCount(run) <= entryCount(rowSet);
    }

    /**
     * The entries {@code rowSet} stores, which is what moving it costs.
     */
    private static long entryCount(final RowSet rowSet) {
        // Every row set the engine builds is one of these. Answering some sentinel for anything else would not be
        // the safe reading it looks like: two of them would compare equal and fold every prepend, which is the
        // quadratic this guard exists to prevent.
        Assert.assertion(rowSet instanceof WritableRowSetImpl, "rowSet instanceof WritableRowSetImpl",
                rowSet.getClass(), "rowSet.getClass()");
        return ((WritableRowSetImpl) rowSet).getInnerSet().ixEntryCount();
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
            SafeCloseable.closeAll(tail);
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

    /** The batch size in force after clamping. */
    @VisibleForTesting
    int batchSize() {
        return batchSize;
    }

    /**
     * Close everything not yet handed over by {@link #build()}.
     */
    @Override
    public void close() {
        run = null;
        groupCount = 0;
        try {
            SafeCloseable.closeAll(entries);
        } finally {
            entries.clear();
        }
    }
}
