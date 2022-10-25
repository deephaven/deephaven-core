package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An implementation of {@link UpdateBy} dedicated to zero key computation.
 */
class UpdateByBucketHelper {
    protected final ColumnSource<?>[] inputSources;
    // some columns will have multiple inputs, such as time-based and Weighted computations
    final int[][] operatorInputSourceSlots;
    final UpdateByOperator[] operators;
    final UpdateByWindow[] windows;
    final QueryTable source;
    final UpdateBy.UpdateByRedirectionContext redirContext;
    final UpdateByControl control;
    final QueryTable result;

    /** An array of {@link UpdateByWindow.UpdateByWindowContext}s for each window */
    final UpdateByWindow.UpdateByWindowContext[] windowContexts;

    /** store timestamp data in an Ssa (if needed) */
    final String timestampColumnName;
    final LongSegmentedSortedArray timestampSsa;
    final ColumnSource<?> timestampColumnSource;
    final ModifiedColumnSet timestampColumnSet;

    /** Indicates this bucket needs to be processed (at least window and operator are dirty) */
    boolean isDirty;

    /**
     * Perform an updateBy without any key columns.
     *
     * @param source the source table
     * @param operators, the operations to perform
     * @param resultSources the result sources
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */

    protected UpdateByBucketHelper(@NotNull final String description,
                                   @NotNull final QueryTable source,
                                   @NotNull final UpdateByOperator[] operators,
                                   @NotNull final UpdateByWindow[] windows,
                                   @NotNull final ColumnSource<?>[] inputSources,
                                   @NotNull final int[][] operatorInputSourceSlots,
                                   @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
                                   @Nullable String timestampColumnName,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
                                   @NotNull final UpdateByControl control) {

        this.source = source;
        this.operators = operators;
        this.windows = windows;
        this.inputSources = inputSources;
        this.operatorInputSourceSlots = operatorInputSourceSlots;
        this.redirContext = redirContext;
        this.control = control;

        result = new QueryTable(source.getRowSet(), resultSources);

        // do we need a timestamp SSA?
        this.timestampColumnName = timestampColumnName;
        if (timestampColumnName != null) {
            this.timestampSsa = new LongSegmentedSortedArray(4096);
            this.timestampColumnSource =
                    ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(this.timestampColumnName));
            this.timestampColumnSet = source.newModifiedColumnSet(timestampColumnName);
        } else {
            this.timestampSsa = null;
            this.timestampColumnSource = null;
            this.timestampColumnSet = null;
        }

        this.windowContexts = new UpdateByWindow.UpdateByWindowContext[windows.length];

        // make a fake update with the initial rows of the table
        final TableUpdateImpl initialUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);

        prepareForUpdate(initialUpdate, true);

        if (source.isRefreshing()) {
            final ZeroKeyUpdateByListener listener = newListener(description, result);
            source.listenForUpdates(listener);
            result.addParentReference(listener);
        }
    }

    ZeroKeyUpdateByListener newListener(@NotNull final String description, @NotNull final QueryTable result) {
        return new ZeroKeyUpdateByListener(description, source);
    }

    private void processUpdateForSsa(TableUpdate upstream) {
        final boolean stampModified = upstream.modifiedColumnSet().containsAny(timestampColumnSet);

        final RowSet restampRemovals;
        final RowSet restampAdditions;

        // modifies are remove + add operations
        if (stampModified) {
            restampAdditions = upstream.added().union(upstream.modified());
            restampRemovals = upstream.removed().union(upstream.getModifiedPreShift());
        } else {
            restampAdditions = upstream.added();
            restampRemovals = upstream.removed();
        }

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
                    upstream.modified().intSize() + Math.max(upstream.added().intSize(), upstream.removed().intSize()),
                    (int) upstream.shifted().getEffectiveSize());
            try (final RowSet fullPrevRowSet = source.getRowSet().copyPrev();
                    final WritableRowSet previousToShift = fullPrevRowSet.minus(restampRemovals);
                    final ColumnSource.GetContext getContext = timestampColumnSource.makeGetContext(size)) {

                // no need to consider upstream removals
                previousToShift.remove(upstream.removed());

                final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                while (sit.hasNext()) {
                    sit.next();
                    try (final RowSet subRowSet = previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
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
        }
    }

    /**
     * Calling this function will prepare this bucket for computation, including making a
     * {@link UpdateByWindow.UpdateByWindowContext} for each window and computing the affected and influencer rowsets
     * for each window
     *
     * @param upstream The incoming update for which to prepare
     * @param initialStep Whether this update is part of the initial creation of the bucket
     */
    public void prepareForUpdate(final TableUpdate upstream, final boolean initialStep) {
        Assert.eqFalse(isDirty, "UpdateBy bucket was marekd dirty before processing an update");

        // add all the SSA data
        if (timestampColumnName != null) {
            processUpdateForSsa(upstream);
        }

        final TrackingRowSet sourceRowSet = source.getRowSet();

        // create context for each window
        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            windowContexts[winIdx] = windows[winIdx].makeWindowContext(
                    sourceRowSet,
                    inputSources,
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

    public boolean isDirty() {
        return isDirty;
    }

    public void processWindow(final int winIdx,
            final ColumnSource<?>[] inputSources,
            final boolean initialStep) {
        // call the window.process() with the correct context for this bucket
        if (windows[winIdx].isWindowDirty(windowContexts[winIdx])) {
            windows[winIdx].processRows(windowContexts[winIdx], inputSources, initialStep);
        }
    }

    public void finalizeUpdate() {
        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            windowContexts[winIdx].close();
            windowContexts[winIdx] = null;
        }
        isDirty = false;
    }

    /**
     * The Listener for apply an upstream {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate) update}
     */
    class ZeroKeyUpdateByListener extends InstrumentedTableUpdateListenerAdapter {
        public ZeroKeyUpdateByListener(@Nullable String description,
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
