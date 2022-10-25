package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedList;
import java.util.Map;

public class ZeroKeyUpdateByManager extends UpdateBy {
    // this manager has only one bucket, managed by this object
    final UpdateByBucketHelper zeroKeyUpdateBy;

    protected ZeroKeyUpdateByManager(@NotNull final String description,
            @NotNull QueryTable source,
            @NotNull UpdateByOperator[] operators,
            @NotNull UpdateByWindow[] windows,
            @NotNull ColumnSource<?>[] inputSources,
            @NotNull int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable String timestampColumnName,
            @NotNull UpdateByRedirectionContext redirContext,
            @NotNull UpdateByControl control) {
        super(description, source, operators, windows, inputSources, operatorInputSourceSlots, resultSources,
                timestampColumnName, redirContext, control);

        // this table will always have the rowset of the source
        result = new QueryTable(source.getRowSet(), resultSources);

        if (source.isRefreshing()) {
            // this is a refreshing source, we will need a listener and recorders
            recorders = new LinkedList<>();
            listener = newListener(description);

            // create an intermediate table that will listen to source updates and shift output columns
            final QueryTable shiftApplyTable = new QueryTable(source.getRowSet(), source.getColumnSourceMap());

            source.listenForUpdates(new BaseTable.ListenerImpl("", source, shiftApplyTable) {
                @Override
                public void onUpdate(@NotNull final TableUpdate upstream) {
                    shiftOutputColumns(upstream);
                    super.onUpdate(upstream);
                }
            });

            // create a recorder instance sourced from the shifting table
            ListenerRecorder shiftRecorder = new ListenerRecorder(description, shiftApplyTable, result);
            shiftRecorder.setMergedListener(listener);
            shiftApplyTable.listenForUpdates(shiftRecorder);
            result.addParentReference(listener);
            recorders.offerLast(shiftRecorder);

            // create input and output modified column sets
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(shiftApplyTable);
                op.createOutputModifiedColumnSet(result);
            }

            // create an updateby bucket instance sourced from the shifting table
            zeroKeyUpdateBy = new UpdateByBucketHelper(description, shiftApplyTable, operators, windows, inputSources,
                    operatorInputSourceSlots, resultSources, timestampColumnName, redirContext, control);
            buckets.offerLast(zeroKeyUpdateBy);

            // create a recorder instance sourced from the bucket helper
            ListenerRecorder recorder = new ListenerRecorder(description, zeroKeyUpdateBy.result, result);
            recorder.setMergedListener(listener);
            zeroKeyUpdateBy.result.listenForUpdates(recorder);
            recorders.offerLast(recorder);
        } else {
            // no shifting will be needed, can create directly from source
            zeroKeyUpdateBy = new UpdateByBucketHelper(description, source, operators, windows, inputSources,
                    operatorInputSourceSlots, resultSources, timestampColumnName, redirContext, control);
            this.result = zeroKeyUpdateBy.result;
            buckets.offerLast(zeroKeyUpdateBy);

            // create input modified column sets only
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(source);
            }
        }

        if (redirContext.isRedirected()) {
            // make a dummy update to generate the initial row keys
            final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY);
            redirContext.processUpdateForRedirection(fakeUpdate, source.getRowSet());
        }

        // do the actual computations
        UpdateByBucketHelper[] dirtyBuckets = new UpdateByBucketHelper[] {zeroKeyUpdateBy};
        processBuckets(dirtyBuckets, true);
        finalizeBuckets(dirtyBuckets);
    }

    // private ColumnSource<?> getCachedColumn(ColumnSource<?> inputSource, final RowSet inputRowSet) {
    // final SparseArrayColumnSource<?> outputSource = SparseArrayColumnSource
    // .getSparseMemoryColumnSource(inputSource.getType(), inputSource.getComponentType());
    //
    // final int CHUNK_SIZE = 1 << 16;
    //
    // try (final RowSequence.Iterator rsIt = inputRowSet.getRowSequenceIterator();
    // final ChunkSink.FillFromContext ffc =
    // outputSource.makeFillFromContext(CHUNK_SIZE);
    // final ChunkSource.GetContext gc = inputSource.makeGetContext(CHUNK_SIZE)) {
    // while (rsIt.hasMore()) {
    // final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
    // final Chunk<? extends Values> values = inputSource.getChunk(gc, chunkOk);
    // outputSource.fillFromChunk(ffc, values, chunkOk);
    // }
    // }
    //
    // return outputSource;
    // }
    //
    // private int[] getAffectedWindows(UpdateByBucketHelper.UpdateContext context) {
    // final TIntArrayList list = new TIntArrayList(windows.length);
    // for (int winIdx = 0; winIdx < windows.length; winIdx++) {
    // if (windows[winIdx].isAffected(context.windowContexts[winIdx])) {
    // list.add(winIdx);
    // }
    // }
    // return list.toArray();
    // }
    //
    // private void addRowSetToInputSourceCache(int srcIdx, final RowSet rowSet) {
    // // create if it doesn't exist
    // if (cachedInputSources[srcIdx] == null) {
    // cachedInputSources[srcIdx] = new CachedInputSource();
    // }
    // cachedInputSources[srcIdx].addRowSet(rowSet);
    // }
    //
    // /**
    // * Examine the buckets and identify the input sources that will benefit from caching. Accumulate the bucket
    // rowsets
    // * for each source independently
    // */
    // private void computeCachedColumnContents(UpdateByBucketHelper[] buckets, boolean initialStep) {
    // // track for each window what sources we need to cache
    // final boolean[] windowSourceCacheNeeded = new boolean[inputSources.length];
    //
    // for (int winIdx = 0; winIdx < windows.length; winIdx++) {
    // Arrays.fill(windowSourceCacheNeeded, false);
    //
    // // for each bucket
    // for (UpdateByBucketHelper bucket : buckets) {
    // UpdateByWindow.UpdateByWindowContext bucketCtx = bucket.windowContexts[winIdx];
    // if (initialStep || (bucket.isDirty && bucketCtx.isDirty)) {
    // for (int opIdx : windows[winIdx].getDirtyOperators(bucketCtx)) {
    // for (int srcIdx : windows[winIdx].getOperatorSourceSlots(opIdx)) {
    // if (inputSourceCacheNeeded[srcIdx]) {
    // windowSourceCacheNeeded[srcIdx] = true;
    // addRowSetToInputSourceCache(srcIdx, bucketCtx.getInfluencerRows());
    // }
    // }
    // }
    // }
    // }
    //
    // // add one to all the reference counts this windows
    // for (int srcIdx = 0; srcIdx < inputSources.length; srcIdx++) {
    // if (windowSourceCacheNeeded[srcIdx]) {
    // cachedInputSources[srcIdx].addReference();
    // }
    // }
    // }
    // }
    //
    // private void maybeCacheInputSources(int[] srcArr) {
    //
    // }
    //
    // private void maybeReleaseInputSources(int[] srcArr) {
    //
    // }
    //
    // /**
    // * The Listener for apply an upstream {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate) update}
    // */
    // class ZeroKeyUpdateByManagerListener extends InstrumentedTableUpdateListenerAdapter {
    // public ZeroKeyUpdateByManagerListener(@Nullable String description,
    // @NotNull final QueryTable source,
    // @NotNull final QueryTable result) {
    // super(description, source, false);
    //
    // }
    //
    // @Override
    // public void onUpdate(TableUpdate upstream) {
    // // do the actual computations
    // processBuckets(false);
    //
    //
    //
    // final boolean windowsModified = ctx.anyModified();
    //
    // if (upstream.modified().isNonempty() || windowsModified) {
    // WritableRowSet modifiedRowSet = RowSetFactory.empty();
    // downstream.modified = modifiedRowSet;
    // if (upstream.modified().isNonempty()) {
    // // Transform any untouched modified columns to the output.
    // transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
    // modifiedRowSet.insert(upstream.modified());
    // }
    //
    //
    //
    // if (windowsModified) {
    // modifiedRowSet.remove(upstream.added());
    // }
    // } else {
    // downstream.modified = RowSetFactory.empty();
    // }
    //
    // // set the modified columns if any operators made changes (add/rem/modify)
    // for (int winIdx = 0; winIdx < windows.length; winIdx++) {
    // if (ctx.windowAffected[winIdx]) {
    // ctx.windowContexts[winIdx].updateOutputModifiedColumnSet(downstream.modifiedColumnSet,
    // windowOperatorOutputModifiedColumnSets[winIdx]);
    // }
    // }
    //
    // result.notifyListeners(downstream);
    // }
    // }


    /**
     * Perform an updateBy without any key columns.
     *
     * @param description the operation description
     * @param source the source table
     * @param operators the operations to perform
     * @param resultSources the result sources
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable final String timestampColumnName,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {

        final ZeroKeyUpdateByManager manager = new ZeroKeyUpdateByManager(description, source, operators, windows,
                inputSources, operatorInputSourceSlots, resultSources, timestampColumnName, redirContext, control);
        return manager.result;
    }
}
