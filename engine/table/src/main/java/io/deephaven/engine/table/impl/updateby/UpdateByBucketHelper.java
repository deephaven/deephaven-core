//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.BiConsumer;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An helper class of {@link UpdateBy} dedicated to zero key computation. This will manage the computation of a single
 * bucket of rows.
 */
class UpdateByBucketHelper extends IntrusiveDoublyLinkedNode.Impl<UpdateByBucketHelper> {
    private static final int SSA_LEAF_SIZE = 4096;
    private final UpdateByWindow[] windows;
    private final String description;
    private final QueryTable source;
    private final UpdateByControl control;
    private final BiConsumer<Throwable, TableListener.Entry> failureNotifier;
    final QueryTable result;

    /** An array of {@link UpdateByWindow.UpdateByWindowBucketContext}s for each window */
    final UpdateByWindow.UpdateByWindowBucketContext[] windowContexts;

    /** store timestamp data in an SSA (if needed) */
    private final String timestampColumnName;
    private final LongSegmentedSortedArray timestampSsa;
    private final ColumnSource<?> timestampColumnSource;
    private final ModifiedColumnSet timestampColumnSet;

    /** Store boxed key values for this bucket */
    private final Object[] bucketKeyValues;

    /** Indicates this bucket needs to be processed (at least one window and operator are dirty) */
    private boolean isDirty;
    /** This rowset will store row keys where the timestamp is not null (will mirror the SSA contents) */
    private final TrackingWritableRowSet timestampValidRowSet;
    /** Track how many rows in this bucket have NULL timestamps */
    private long nullTimestampCount;

    /**
     * Perform updateBy operations on a single bucket of data (either zero-key or already limited through partitioning)
     *
     * @param description describes this bucket helper
     * @param source the source table
     * @param resultSources the result sources
     * @param timestampColumnName the timestamp column used for time-based operations
     * @param control the control object.
     * @param failureNotifier a consumer to notify of any failures
     * @param bucketKeyValues the key values for this bucket (empty for zero-key)
     */
    protected UpdateByBucketHelper(
            @NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable final String timestampColumnName,
            @NotNull final UpdateByControl control,
            @NotNull final BiConsumer<Throwable, TableListener.Entry> failureNotifier,
            @NotNull final Object[] bucketKeyValues) {
        this.description = description;
        this.source = source;
        // some columns will have multiple inputs, such as time-based and Weighted computations
        this.windows = windows;
        this.control = control;
        this.failureNotifier = failureNotifier;
        this.bucketKeyValues = bucketKeyValues;

        result = new QueryTable(source.getRowSet(), resultSources);

        // do we need a timestamp SSA?
        this.timestampColumnName = timestampColumnName;
        if (timestampColumnName != null) {
            this.timestampSsa = new LongSegmentedSortedArray(SSA_LEAF_SIZE);
            this.timestampColumnSource =
                    ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(this.timestampColumnName));
            this.timestampColumnSet = source.newModifiedColumnSet(timestampColumnName);
            this.timestampValidRowSet = source.getRowSet().copy().toTracking();
        } else {
            this.timestampSsa = null;
            this.timestampColumnSource = null;
            this.timestampColumnSet = null;
            this.timestampValidRowSet = null;
        }

        this.windowContexts = new UpdateByWindow.UpdateByWindowBucketContext[windows.length];

        // make a fake update with the initial rows of the table
        final TableUpdateImpl initialUpdate = new TableUpdateImpl(
                source.getRowSet().copy(), // send a copy since this will be closed by release()
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);

        prepareForUpdate(initialUpdate, true);

        initialUpdate.release();

        if (source.isRefreshing()) {
            final UpdateByBucketHelperListener listener = newListener(description);
            source.addUpdateListener(listener);
            result.addParentReference(listener);
        }
    }

    UpdateByBucketHelperListener newListener(@NotNull final String description) {
        return new UpdateByBucketHelperListener(description, source);
    }

    private void processUpdateForSsa(final TableUpdate upstream, final boolean timestampsModified) {
        if (upstream.empty()) {
            return;
        }

        final int chunkSize = 1 << 12; // 4k

        try (final RowSet addedAndModified = timestampsModified ? upstream.added().union(upstream.modified()) : null;
                final RowSet removedAndModifiedPreShift =
                        timestampsModified ? upstream.removed().union(upstream.getModifiedPreShift()) : null;
                final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(chunkSize);
                final WritableLongChunk<? extends Values> ssaValues = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedRowKeys> ssaKeys = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedRowKeys> nullTsKeys = WritableLongChunk.makeWritableChunk(chunkSize)) {

            final RowSet restampAdditions = timestampsModified ? addedAndModified : upstream.added();
            final RowSet restampRemovals = timestampsModified ? removedAndModifiedPreShift : upstream.removed();

            // removes
            if (restampRemovals.isNonempty()) {
                try (final RowSequence.Iterator it = restampRemovals.getRowSequenceIterator()) {

                    MutableLong lastTimestamp = new MutableLong(NULL_LONG);
                    while (it.hasMore()) {
                        RowSequence chunkRs = it.getNextRowSequenceWithLength(4096);

                        // Get the chunks for values and keys.
                        LongChunk<? extends Values> valuesChunk =
                                timestampColumnSource.getPrevChunk(context, chunkRs).asLongChunk();
                        LongChunk<OrderedRowKeys> keysChunk = chunkRs.asRowKeyChunk();

                        // Push only non-null values/keys into the Ssa.
                        final int chunkNullCount = fillChunkWithNonNull(keysChunk, valuesChunk, ssaKeys, ssaValues,
                                nullTsKeys, lastTimestamp);
                        nullTimestampCount -= chunkNullCount;

                        // Only process if there are non-nulls in this chunk.
                        if (chunkNullCount != keysChunk.size()) {
                            timestampSsa.remove(ssaValues, ssaKeys);

                            // If we have removed all the nulls, we will reset to mirror the source. Otherwise, need to
                            // remove these rows from the non-null set. This test skips the transition chunk's removals.
                            if (nullTimestampCount > 0) {
                                // Update the non-null set with these removes.
                                timestampValidRowSet.remove(ssaKeys, 0, ssaKeys.size());
                            }
                        }
                    }
                }
            }

            // If we have no nulls, we can safely and efficiently mirror the source.
            final boolean resetToSource = nullTimestampCount == 0;
            if (resetToSource) {
                timestampValidRowSet.resetTo(source.getRowSet());
            }

            // shifts
            if (upstream.shifted().nonempty()) {
                if (!resetToSource) {
                    upstream.shifted().apply(timestampValidRowSet);
                }

                final int size = Math.max(
                        upstream.modified().intSize() + Math.max(upstream.added().intSize(),
                                upstream.removed().intSize()),
                        (int) upstream.shifted().getEffectiveSize());

                try (final WritableRowSet previousToShift = source.getRowSet().prev().minus(restampRemovals);
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
                            if (sit.polarityReversed()) {
                                timestampSsa.applyShiftReverse(shiftValues, subRowSet.asRowKeyChunk(),
                                        sit.shiftDelta());
                            } else {
                                timestampSsa.applyShift(shiftValues, subRowSet.asRowKeyChunk(), sit.shiftDelta());
                            }
                        }
                    }
                }
            }


            // adds
            if (restampAdditions.isNonempty()) {
                try (final RowSequence.Iterator it = restampAdditions.getRowSequenceIterator()) {
                    MutableLong lastTimestamp = new MutableLong(NULL_LONG);

                    while (it.hasMore()) {
                        RowSequence chunkRs = it.getNextRowSequenceWithLength(chunkSize);

                        // Get the chunks for values and keys.
                        LongChunk<? extends Values> valuesChunk =
                                timestampColumnSource.getChunk(context, chunkRs).asLongChunk();
                        LongChunk<OrderedRowKeys> keysChunk = chunkRs.asRowKeyChunk();

                        // Push only non-null values/keys into the Ssa.
                        final int chunkNullCount = fillChunkWithNonNull(keysChunk, valuesChunk, ssaKeys, ssaValues,
                                nullTsKeys, lastTimestamp);
                        nullTimestampCount += chunkNullCount;

                        // Only add to the SSA if there are non-nulls in this chunk.
                        if (chunkNullCount != keysChunk.size()) {
                            timestampSsa.insert(ssaValues, ssaKeys);
                        }

                        if (resetToSource) {
                            // The original source rowset might contain null-ts rows. Remove them now.
                            if (chunkNullCount > 0) {
                                timestampValidRowSet.remove(nullTsKeys, 0, nullTsKeys.size());
                            }
                        } else {
                            // Maintain the parallel non-null set by adding the new non-null values.
                            timestampValidRowSet.insert(ssaKeys, 0, ssaKeys.size());
                        }
                    }
                }
            }
        }

        Assert.eq(nullTimestampCount, "nullTimestampCount", source.size() - timestampValidRowSet.size());
    }

    /**
     * Helper function to fill a LongChunk while skipping values that are NULL_LONG. Used to populate an SSA from a
     * source containing null values
     *
     * @return the number of NULL values found in the set
     */
    private int fillChunkWithNonNull(LongChunk<OrderedRowKeys> keysChunk, LongChunk<? extends Values> valuesChunk,
            WritableLongChunk<OrderedRowKeys> ssaKeys, WritableLongChunk<? extends Values> ssaValues,
            WritableLongChunk<OrderedRowKeys> nullTimestampKeys, MutableLong lastTimestamp) {
        int nullCount = 0;

        // reset the insertion chunks
        ssaValues.setSize(0);
        ssaKeys.setSize(0);
        nullTimestampKeys.setSize(0);

        // add only non-null timestamps to this Ssa
        for (int i = 0; i < valuesChunk.size(); i++) {
            long ts = valuesChunk.get(i);
            if (ts == NULL_LONG) {
                // track the nulls added during this operation
                nullTimestampKeys.add(keysChunk.get(i));
                nullCount++;
                continue;
            }
            if (ts < lastTimestamp.get()) {
                throw (new TableDataException(
                        "Timestamp values in UpdateBy operators must not decrease"));
            }

            ssaValues.add(ts);
            ssaKeys.add(keysChunk.get(i));

            // store the current ts for comparison
            lastTimestamp.set(ts);
        }
        return nullCount;
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
        final boolean timestampsModified;

        // add all the SSA data
        if (timestampColumnName != null) {
            // test whether any timestamps were modified
            timestampsModified =
                    upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(timestampColumnSet);

            processUpdateForSsa(upstream, timestampsModified);
        } else {
            timestampsModified = false;
        }

        final TrackingRowSet sourceRowSet = source.getRowSet();

        // create context for each window
        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            windowContexts[winIdx] = windows[winIdx].makeWindowContext(
                    sourceRowSet,
                    timestampColumnSource,
                    timestampSsa,
                    timestampValidRowSet,
                    timestampsModified,
                    control.chunkCapacityOrDefault(),
                    initialStep,
                    bucketKeyValues);

            // compute the affected/influenced operators and rowsets within this window
            windows[winIdx].computeAffectedRowsAndOperators(windowContexts[winIdx], upstream);

            isDirty |= windows[winIdx].isWindowBucketDirty(windowContexts[winIdx]);
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
     * Close the window contexts and release resources for this bucket
     */
    public void finalizeUpdate() {
        SafeCloseableArray.close(windowContexts);
        isDirty = false;
    }

    /**
     * The Listener that accepts an {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate) update} and
     * prepares this bucket for processing. This includes determination of `isDirty` status and the computation of
     * `affected` and `influencer` row sets for this processing cycle.
     */
    private class UpdateByBucketHelperListener extends InstrumentedTableUpdateListenerAdapter {

        private UpdateByBucketHelperListener(@Nullable final String description, @NotNull final QueryTable source) {
            super(description, source, false);
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            prepareForUpdate(upstream, false);
        }

        @Override
        public void onFailureInternal(@NotNull final Throwable originalException, @Nullable final Entry sourceEntry) {
            failureNotifier.accept(originalException, sourceEntry);
        }
    }
}
