package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An helper class of {@link UpdateBy} dedicated to zero key computation. This will manage the computation of a single
 * bucket of rows.
 */
class UpdateByBucketHelper extends IntrusiveDoublyLinkedNode.Impl<UpdateByBucketHelper> {
    private static final int SSA_LEAF_SIZE = 4096;
    protected final ColumnSource<?>[] inputSources;
    // some columns will have multiple inputs, such as time-based and Weighted computations
    final int[][] operatorInputSourceSlots;
    final UpdateByOperator[] operators;
    final UpdateByWindow[] windows;
    final QueryTable source;
    final RowRedirection rowRedirection;
    final UpdateByControl control;
    final QueryTable result;

    /** An array of {@link UpdateByWindow.UpdateByWindowBucketContext}s for each window */
    final UpdateByWindow.UpdateByWindowBucketContext[] windowContexts;

    /** store timestamp data in an SSA (if needed) */
    final String timestampColumnName;
    final LongSegmentedSortedArray timestampSsa;
    final ColumnSource<?> timestampColumnSource;
    final ModifiedColumnSet timestampColumnSet;

    /** Indicates this bucket needs to be processed (at least one window and operator are dirty) */
    boolean isDirty;

    /**
     * Perform updateBy operations on a single bucket of data (either zero-key or already limited through partitioning)
     *
     * @param description descibes this bucket helper
     * @param source the source table
     * @param operators, the operations to perform
     * @param inputSources the source input sources
     * @param operatorInputSourceSlots the mapping from operator index to needed input source indices
     * @param resultSources the result sources
     * @param timestampColumnName the timestamp column used for time-based operations
     * @param rowRedirection the row redirection for operator output columns
     * @param control the control object.
     */

    protected UpdateByBucketHelper(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable String timestampColumnName,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {

        this.source = source;
        this.operators = operators;
        this.windows = windows;
        this.inputSources = inputSources;
        this.operatorInputSourceSlots = operatorInputSourceSlots;
        this.rowRedirection = rowRedirection;
        this.control = control;

        result = new QueryTable(source.getRowSet(), resultSources);

        // do we need a timestamp SSA?
        this.timestampColumnName = timestampColumnName;
        if (timestampColumnName != null) {
            this.timestampSsa = new LongSegmentedSortedArray(SSA_LEAF_SIZE);
            this.timestampColumnSource =
                    ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(this.timestampColumnName));
            this.timestampColumnSet = source.newModifiedColumnSet(timestampColumnName);
        } else {
            this.timestampSsa = null;
            this.timestampColumnSource = null;
            this.timestampColumnSet = null;
        }

        this.windowContexts = new UpdateByWindow.UpdateByWindowBucketContext[windows.length];

        // make a fake update with the initial rows of the table
        final TableUpdateImpl initialUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);

        prepareForUpdate(initialUpdate, true);

        if (source.isRefreshing()) {
            final UpdateByBucketHelperListener listener = newListener(description);
            source.addUpdateListener(listener);
            result.addParentReference(listener);
        }
    }

    UpdateByBucketHelperListener newListener(@NotNull final String description) {
        return new UpdateByBucketHelperListener(description, source);
    }

    private void processUpdateForSsa(TableUpdate upstream) {
        final boolean stampModified = upstream.modifiedColumnSet().containsAny(timestampColumnSet);

        try (final RowSet restampAdditions =
                stampModified ? upstream.added().union(upstream.modified()) : upstream.added().copy();
                final RowSet restampRemovals = stampModified ? upstream.removed().union(upstream.getModifiedPreShift())
                        : upstream.removed().copy()) {
            // removes
            if (restampRemovals.isNonempty()) {
                final int size = (int) Math.min(restampRemovals.size(), 4096);
                try (final RowSequence.Iterator it = restampRemovals.getRowSequenceIterator();
                        final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(size);
                        final WritableLongChunk<? extends Values> ssaValues = WritableLongChunk.makeWritableChunk(size);
                        final WritableLongChunk<OrderedRowKeys> ssaKeys = WritableLongChunk.makeWritableChunk(size)) {

                    MutableLong lastTimestamp = new MutableLong(NULL_LONG);
                    while (it.hasMore()) {
                        RowSequence chunkRs = it.getNextRowSequenceWithLength(4096);

                        // get the chunks for values and keys
                        LongChunk<? extends Values> valuesChunk =
                                timestampColumnSource.getPrevChunk(context, chunkRs).asLongChunk();
                        LongChunk<OrderedRowKeys> keysChunk = chunkRs.asRowKeyChunk();

                        // push only non-null values/keys into the Ssa
                        fillChunkWithNonNull(keysChunk, valuesChunk, ssaKeys, ssaValues, lastTimestamp);
                        timestampSsa.remove(ssaValues, ssaKeys);
                    }
                }
            }

            // shifts
            if (upstream.shifted().nonempty()) {
                final int size = Math.max(
                        upstream.modified().intSize()
                                + Math.max(upstream.added().intSize(), upstream.removed().intSize()),
                        (int) upstream.shifted().getEffectiveSize());
                try (final RowSet fullPrevRowSet = source.getRowSet().copyPrev();
                        final WritableRowSet previousToShift = fullPrevRowSet.minus(restampRemovals);
                        final ColumnSource.GetContext getContext = timestampColumnSource.makeGetContext(size)) {

                    final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                    while (sit.hasNext()) {
                        sit.next();
                        try (final RowSet subRowSet =
                                previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
                            if (subRowSet.isEmpty()) {
                                continue;
                            }

                            final LongChunk<? extends Values> shiftValues =
                                    timestampColumnSource.getPrevChunk(getContext, subRowSet).asLongChunk();

                            timestampSsa.applyShiftReverse(shiftValues, subRowSet.asRowKeyChunk(), sit.shiftDelta());
                        }
                    }
                }
            }

            // adds
            if (restampAdditions.isNonempty()) {
                final int size = (int) Math.min(restampAdditions.size(), 4096);
                try (final RowSequence.Iterator it = restampAdditions.getRowSequenceIterator();
                        final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(size);
                        final WritableLongChunk<? extends Values> ssaValues = WritableLongChunk.makeWritableChunk(size);
                        final WritableLongChunk<OrderedRowKeys> ssaKeys = WritableLongChunk.makeWritableChunk(size)) {
                    MutableLong lastTimestamp = new MutableLong(NULL_LONG);
                    while (it.hasMore()) {
                        RowSequence chunkRs = it.getNextRowSequenceWithLength(4096);

                        // get the chunks for values and keys
                        LongChunk<? extends Values> valuesChunk =
                                timestampColumnSource.getChunk(context, chunkRs).asLongChunk();
                        LongChunk<OrderedRowKeys> keysChunk = chunkRs.asRowKeyChunk();

                        // push only non-null values/keys into the Ssa
                        fillChunkWithNonNull(keysChunk, valuesChunk, ssaKeys, ssaValues, lastTimestamp);
                        timestampSsa.insert(ssaValues, ssaKeys);
                    }
                }
            }
        }
    }

    /**
     * helper function to fill a LongChunk while skipping values that are NULL_LONG. Used to populate an SSA from a
     * source containing null values
     */
    private void fillChunkWithNonNull(LongChunk<OrderedRowKeys> keysChunk, LongChunk<? extends Values> valuesChunk,
            WritableLongChunk<OrderedRowKeys> ssaKeys, WritableLongChunk<? extends Values> ssaValues,
            MutableLong lastTimestamp) {
        // reset the insertion chunks
        ssaValues.setSize(0);
        ssaKeys.setSize(0);

        // add only non-null timestamps to this Ssa
        for (int i = 0; i < valuesChunk.size(); i++) {
            long ts = valuesChunk.get(i);
            if (ts < lastTimestamp.longValue()) {
                throw (new IllegalStateException(
                        "updateBy time-based operators require non-descending timestamp values"));
            }
            if (ts != NULL_LONG) {
                ssaValues.add(ts);
                ssaKeys.add(keysChunk.get(i));
            }
            // store the current ts for comparison
            lastTimestamp.setValue(ts);
        }
    }

    /**
     * Calling this function will prepare this bucket for computation, including making a
     * {@link UpdateByWindow.UpdateByWindowBucketContext} for each window and computing the affected and influencer
     * rowsets for each window
     *
     * @param upstream The incoming update for which to prepare
     * @param initialStep Whether this update is part of the initial creation of the bucket
     */
    public void prepareForUpdate(final TableUpdate upstream, final boolean initialStep) {
        Assert.eqFalse(isDirty, "UpdateBy bucket was marked dirty before processing an update");

        // add all the SSA data
        if (timestampColumnName != null) {
            processUpdateForSsa(upstream);
        }

        final TrackingRowSet sourceRowSet = source.getRowSet();

        // create context for each window
        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            windowContexts[winIdx] = windows[winIdx].makeWindowContext(
                    sourceRowSet,
                    timestampColumnSource,
                    timestampSsa,
                    control.chunkCapacityOrDefault(),
                    initialStep);

            // compute the affected/influenced operators and rowsets within this window
            windows[winIdx].computeAffectedRowsAndOperators(windowContexts[winIdx], upstream);

            isDirty |= windows[winIdx].isWindowDirty(windowContexts[winIdx]);
        }

        if (!isDirty) {
            // we will never use these contexts, so clean them up now
            finalizeUpdate();
        }
    }

    /**
     * Returns {@code true} if this bucket needs to be processed (at least one operator and window has changes)
     */
    public boolean isDirty() {
        return isDirty;
    }

    /**
     * Store an array of input sources for the following call to {@code processWindow()}. The function allows for the
     * use of cached input sources instead of the original input sources.
     *
     * @param winIdx the index of the window to modify
     * @param inputSources the input sources for the operators
     */
    public void assignInputSources(final int winIdx, final ColumnSource<?>[] inputSources) {
        windows[winIdx].assignInputSources(windowContexts[winIdx], inputSources);
    }

    /**
     * Perform all the operator calculations for this bucket using the input sources assigned by the
     * {@code assignInputSources()} call.
     *
     * @param winIdx the index of the window to modify
     * @param initialStep indicates whether this is part of the creation phase
     */
    public void processWindow(final int winIdx, final boolean initialStep) {
        if (!windows[winIdx].isWindowDirty(windowContexts[winIdx])) {
            return; // no work to do for this bucket window
        }
        windows[winIdx].processRows(windowContexts[winIdx], initialStep);
    }

    /**
     * Close the window contexts and release resources for this bucket
     */
    public void finalizeUpdate() {
        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            windowContexts[winIdx].close();
            windowContexts[winIdx] = null;
        }
        isDirty = false;
    }

    /**
     * The Listener that accepts an {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate) update} and
     * prepares this bucket for processing. This includes determination of `isDirty` status and the computation of
     * `affected` and `influencer` row sets for this processing cycle.
     */
    class UpdateByBucketHelperListener extends InstrumentedTableUpdateListenerAdapter {
        public UpdateByBucketHelperListener(@Nullable String description,
                @NotNull final QueryTable source) {
            super(description, source, false);
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            prepareForUpdate(upstream, false);

            // pass the update unchanged, just increment the ref count
            TableUpdate downstream = upstream.acquire();
            result.notifyListeners(downstream);
        }
    }
}
