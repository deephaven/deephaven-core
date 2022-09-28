package io.deephaven.engine.table.impl;

import gnu.trove.set.hash.TLongHashSet;
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
import io.deephaven.engine.table.impl.util.UpdateSizeCalculator;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An implementation of {@link UpdateBy} dedicated to zero key computation.
 */
class ZeroKeyUpdateBy extends UpdateBy {
    /** Apply shifts to operator outputs? */
    final boolean applyShifts;

    /** store timestamp data in an Ssa (if needed) */
    final String timestampColumnName;
    final LongSegmentedSortedArray timestampSsa;
    final ColumnSource<?> timestampColumnSource;
    final ModifiedColumnSet timestampColumnSet;

    /**
     * Perform an updateBy without any key columns.
     *
     * @param description the operation description
     * @param source the source table
     * @param ops the operations to perform
     * @param resultSources the result sources
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] ops,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control,
            final boolean applyShifts) {
        final QueryTable result = new QueryTable(source.getRowSet(), resultSources);
        final ZeroKeyUpdateBy updateBy = new ZeroKeyUpdateBy(ops, source, redirContext, control, applyShifts);
        updateBy.doInitialAdditions();

        if (source.isRefreshing()) {
            final ZeroKeyUpdateByListener listener = updateBy.newListener(description, result);
            source.listenForUpdates(listener);
            result.addParentReference(listener);
        }

        return result;
    }

    protected ZeroKeyUpdateBy(@NotNull final UpdateByOperator[] operators,
            @NotNull final QueryTable source,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control,
            final boolean applyShifts) {
        super(operators, source, redirContext, control);

        // do we need a timestamp SSA?
        this.timestampColumnName = Arrays.stream(operators)
                .filter(op -> op.getTimestampColumnName() != null)
                .map(UpdateByOperator::getTimestampColumnName)
                .findFirst().orElse(null);

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
        this.applyShifts = applyShifts;
    }

    ZeroKeyUpdateByListener newListener(@NotNull final String description, @NotNull final QueryTable result) {
        return new ZeroKeyUpdateByListener(description, source, result);
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
                    final RowSet previousToShift = fullPrevRowSet.minus(restampRemovals);
                    final ColumnSource.GetContext getContext = timestampColumnSource.makeGetContext(size)) {

                final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                while (sit.hasNext()) {
                    sit.next();
                    try (final RowSet subRowSet = previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange());
                            final RowSet rowSetToShift = subRowSet.minus(upstream.removed())) {
                        if (rowSetToShift.isEmpty()) {
                            continue;
                        }

                        final LongChunk<? extends Values> shiftValues =
                                timestampColumnSource.getPrevChunk(getContext, rowSetToShift).asLongChunk();

                        timestampSsa.applyShiftReverse(shiftValues, rowSetToShift.asRowKeyChunk(), sit.shiftDelta());
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


    void doInitialAdditions() {
        final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.ALL);
        if (redirContext.isRedirected()) {
            redirContext.processUpdateForRedirection(fakeUpdate, source.getRowSet());
        }

        // add all the SSA data
        if (timestampColumnName != null) {
            processUpdateForSsa(fakeUpdate);
        }

        // do the processing for this fake update
        try (final UpdateContext ctx = new UpdateContext(fakeUpdate, true)) {
            ctx.processRows();
        }
    }

    /**
     * An object to hold the transient state during a single
     * {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate)} update cycle.
     */
    private class UpdateContext implements SafeCloseable {
        /** The expected size of chunks to the various update stages */
        int chunkSize;

        /** A {@link SharedContext} to be used while creating other contexts */
        SharedContext sharedContext = SharedContext.makeSharedContext();

        /** An array of {@link UpdateByWindow.UpdateByWindowContext}s for each input column */
        final UpdateByWindow.UpdateByWindowContext[] windowContexts;

        /** Indicate if any of the operators in this window are affected by the update. */
        boolean[] windowAffected;

        /** A sharedContext to be used with previous values */
        SharedContext prevSharedContext;

        @SuppressWarnings("resource")
        UpdateContext(@NotNull final TableUpdate upstream,
                final boolean isInitializeStep) {
            final int updateSize = UpdateSizeCalculator.chunkSize(upstream, control.chunkCapacityOrDefault());

            this.chunkSize =
                    UpdateSizeCalculator.chunkSize(updateSize, upstream.shifted(), control.chunkCapacityOrDefault());

            this.windowContexts = new UpdateByWindow.UpdateByWindowContext[windows.length];
            this.windowAffected = new boolean[windows.length];

            for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                // create a context for each window
                windowContexts[winIdx] = windows[winIdx].makeWindowContext(
                        source.getRowSet(),
                        inputSources,
                        timestampColumnSource,
                        timestampSsa,
                        chunkSize,
                        isInitializeStep);

                // compute the affected/influenced operators and rowset within this window
                windowAffected[winIdx] = windowContexts[winIdx].computeAffectedRowsAndOperators(upstream);
            }
        }

        public SharedContext getSharedContext() {
            return sharedContext;
        }

        public boolean anyModified() {
            for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                if (windowContexts[winIdx].anyModified()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() {
            sharedContext.close();

            if (prevSharedContext != null) {
                prevSharedContext.close();
            }

            for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                if (windowContexts[winIdx] != null) {
                    windowContexts[winIdx].close();
                }
            }
        }

        private void processRows() {
            // this might be parallelized if there are multiple windows
            for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                if (windowAffected[winIdx]) {
                    // this will internally call initialize() and finish() for each operator
                    windowContexts[winIdx].processRows();
                }
            }
        }
    }

    /**
     * The Listener for apply an upstream {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate) update}
     */
    class ZeroKeyUpdateByListener extends InstrumentedTableUpdateListenerAdapter {
        private final QueryTable result;
        private final ModifiedColumnSet.Transformer transformer;

        public ZeroKeyUpdateByListener(@Nullable String description,
                @NotNull final QueryTable source,
                @NotNull final QueryTable result) {
            super(description, source, false);
            this.result = result;

            for (int ii = 0; ii < windows.length; ii++) {
                windows[ii].startTrackingModifications(source, result);
            }

            this.transformer =
                    source.newModifiedColumnSetTransformer(result, source.getDefinition().getColumnNamesArray());
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            // update the Ssa
            if (timestampColumnName != null) {
                processUpdateForSsa(upstream);
            }

            try (final UpdateContext ctx = new UpdateContext(upstream, false)) {
                if (applyShifts) {
                    if (redirContext.isRedirected()) {
                        redirContext.processUpdateForRedirection(upstream, source.getRowSet());
                    } else {
                        // We will not mess with shifts if we are using a redirection because we'll have applied the
                        // shift to the redirection index already by now.
                        if (upstream.shifted().nonempty()) {
                            try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                                upstream.shifted().apply((begin, end, delta) -> {
                                    try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                                        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                                            operators[opIdx].applyOutputShift(subRowSet, delta);
                                        }
                                    }
                                });
                            }
                        }
                    }
                }

                // Now do the processing
                ctx.processRows();

                final TableUpdateImpl downstream = new TableUpdateImpl();
                // copy these rowSets since TableUpdateImpl#reset will close them with the upstream update
                downstream.added = upstream.added().copy();
                downstream.removed = upstream.removed().copy();
                downstream.shifted = upstream.shifted();

                downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                downstream.modifiedColumnSet.clear();

                final boolean windowsModified = ctx.anyModified();

                if (upstream.modified().isNonempty() || windowsModified) {
                    WritableRowSet modifiedRowSet = RowSetFactory.empty();
                    downstream.modified = modifiedRowSet;
                    if (upstream.modified().isNonempty()) {
                        // Transform any untouched modified columns to the output.
                        transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                        modifiedRowSet.insert(upstream.modified());
                    }

                    // retrieve the modified rowsets from the windows
                    for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                        if (ctx.windowAffected[winIdx]) {
                            if (ctx.windowContexts[winIdx].anyModified()) {
                                modifiedRowSet.insert(ctx.windowContexts[winIdx].getModifiedRows());
                            }
                        }
                    }

                    if (windowsModified) {
                        modifiedRowSet.remove(upstream.added());
                    }
                } else {
                    downstream.modified = RowSetFactory.empty();
                }

                // set the modified columns if any operators made changes (add/rem/modify)
                for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                    if (ctx.windowAffected[winIdx]) {
                        ctx.windowContexts[winIdx].updateOutputModifiedColumnSet(downstream.modifiedColumnSet);
                    }
                }

                result.notifyListeners(downstream);
            }
        }
    }
}
