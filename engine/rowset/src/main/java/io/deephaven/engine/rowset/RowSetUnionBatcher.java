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
 * {@link RowSetFactory#union(Collection) union}, which sorts itself first. The batch size bounds what that buys: a
 * caller that already knows how many row sets it will produce passes that count and merges exactly once.
 *
 * <p>
 * Two things are handled here rather than at the merge. {@link RowSet#isEmpty() Empty} row sets are dropped as they
 * arrive, so a batch never spends a slot on one. A row set that only appends past the end of the previous one is
 * spliced onto it in place instead of taking a slot of its own, which the row set implementations satisfy without
 * merging range by range; input that arrives in ascending order therefore collapses into a single row set that
 * {@link #build()} hands over as it stands, with no union at all.
 *
 * <p>
 * {@link #build()} is what releases the result to the caller. Everything this batcher is still holding at
 * {@link #close()} is closed, so a caller drives it from a try-with-resources block and a traversal that throws part
 * way through abandons what it had gathered rather than handing back half a union.
 */
public final class RowSetUnionBatcher implements SafeCloseable {

    /**
     * Row sets to gather before merging when the caller has no count to go on, which is to say when the input is driven
     * by data rather than by the shape of the query. Large enough that the merge amortizes the pass it costs, small
     * enough to bound what is held at once.
     */
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final int batchSize;
    private final List<WritableRowSet> batch;

    /**
     * Everything merged so far, or null while the batch still holds all of it. {@link #build()} hands this over.
     */
    private WritableRowSet accumulated;

    /**
     * The last entry of {@link #batch}, which a row set that appends to it is spliced onto. Null whenever the batch is
     * empty.
     */
    private WritableRowSet run;

    /**
     * @param batchSize The number of row sets to gather before merging, at least one
     */
    public RowSetUnionBatcher(final int batchSize) {
        this.batchSize = Math.max(1, batchSize);
        batch = new ArrayList<>(this.batchSize);
    }

    /**
     * Add {@code rowSet} to the union, merging the outstanding batch if it is now full.
     *
     * <p>
     * Ownership of {@code rowSet} passes here: it may be closed before this call returns, and the caller must not use
     * or close it afterwards. Use {@link #addCopy(RowSet)} for a row set the caller keeps.
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
     * Add the current contents of {@code rowSet} to the union, merging the outstanding batch if it is now full.
     *
     * <p>
     * The caller retains ownership of {@code rowSet} and remains responsible for closing it. What this batcher retains
     * is a snapshot: a {@link RowSet#copy() copy-on-write reference} when it needs one, which later mutation of
     * {@code rowSet} does not disturb.
     *
     * @param rowSet The row set to add; the caller retains ownership of it
     */
    public void addCopy(@NotNull final RowSet rowSet) {
        if (rowSet.isEmpty()) {
            return;
        }
        if (appendsToRun(rowSet)) {
            run.insert(rowSet);
            return;
        }
        startRun(rowSet.copy());
    }

    /**
     * Merge whatever is outstanding and hand over the union of everything added since this batcher was constructed or
     * last built. Ownership passes to the caller, and this batcher is left empty and ready for more.
     *
     * @return A new {@link WritableRowSet} containing every row key added
     */
    public WritableRowSet build() {
        mergeBatch();
        final WritableRowSet result = accumulated;
        accumulated = null;
        return result == null ? RowSetFactory.empty() : result;
    }

    /**
     * Whether inserting {@code rowSet} into the run only extends it past its last row key, which the row set
     * implementations satisfy by splicing rather than by merging range by range.
     */
    private boolean appendsToRun(final RowSet rowSet) {
        return run != null && rowSet.firstRowKey() > run.lastRowKey();
    }

    private void startRun(final WritableRowSet rowSet) {
        batch.add(rowSet);
        run = rowSet;
        if (batch.size() >= batchSize) {
            mergeBatch();
        }
    }

    private void mergeBatch() {
        run = null;
        if (batch.isEmpty()) {
            return;
        }
        if (batch.size() == 1) {
            // A straight run of appends built this one row set on the way in, so there is nothing left to merge.
            final WritableRowSet only = batch.remove(0);
            if (accumulated == null) {
                accumulated = only;
            } else {
                try (only) {
                    accumulated.insert(only);
                }
            }
            return;
        }
        if (accumulated == null) {
            // Inserting into an empty row set adopts what it is handed, so starting empty costs nothing here.
            accumulated = RowSetFactory.empty();
        }
        RowSetFactory.insertUnionAndClose(accumulated, batch);
    }

    /**
     * The number of row sets the outstanding batch holds, which is what the next merge will be handed. A run of appends
     * does not grow it.
     */
    @VisibleForTesting
    int pendingBatchSize() {
        return batch.size();
    }

    /**
     * Close everything not yet handed over by {@link #build()}.
     */
    @Override
    public void close() {
        run = null;
        final WritableRowSet localAccumulated = accumulated;
        accumulated = null;
        try {
            SafeCloseable.closeAll(batch.iterator());
        } finally {
            batch.clear();
            if (localAccumulated != null) {
                localAccumulated.close();
            }
        }
    }
}
