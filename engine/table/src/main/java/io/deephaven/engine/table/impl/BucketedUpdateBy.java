package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper;
import io.deephaven.engine.table.impl.by.HashedRunFinder;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.sort.permute.LongPermuteKernel;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.updateby.hashing.*;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.table.impl.util.UpdateSizeCalculator;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Arrays;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public class BucketedUpdateBy extends UpdateBy {
    /** The column sources that are used as keys for bucketing values. */
    private final ColumnSource<?>[] keySources;

    /** The hash table object to store key tuples -> bucket positions */
    private UpdateByStateManager hashTable;

    /** A tracker object to manage indices and updates on a per-bucket basis */
    private final UpdateBySlotTracker slotTracker;

    /** An object to keep track of the next available output position to use */
    final MutableInt nextOutputPosition = new MutableInt(0);

    /**
     * An object to hold the transient state during a single {@link BaseTable.ListenerImpl#onUpdate(TableUpdate)} update
     * cycle.
     */
    private class BucketedContext implements SafeCloseable {
        /** The expected size of chunks to the various update stages */
        int chunkSize;

        /** An indicator of if each slot has been populated with data or not for this phase. */
        boolean[] inputChunkPopulated;

        /** An array of boolean denoting which operators are affected by the current update. */
        final boolean[] opAffected;

        /** A simplem indicator if any operator had modified input columns, adds, or removes. */
        boolean anyAffected;

        /** True if any of the key columns contained modifications. */
        final boolean keysModified;
        /** An array of context objects for each underlying operator */
        final UpdateByOperator.UpdateContext[] opContext;

        /** A {@link SharedContext} to be used while creating other contexts */
        SharedContext sharedContext = SharedContext.makeSharedContext();

        /** An array of {@link ChunkSource.FillContext}s for each input column */
        final SizedSafeCloseable<ChunkSource.FillContext>[] fillContexts;

        /** The kernels used to permute chunks when dealing with updates. */
        final PermuteKernel[] permuteKernels;

        /** A set of chunks used to store post-shift working values */
        final SizedSafeCloseable<WritableChunk<Values>>[] postWorkingChunks;

        /** A set of chunks used to store post-shift working values after a permutation */
        final SizedSafeCloseable<WritableChunk<Values>>[] permutedPostWorkingChunks;

        /** A Chunk of longs to store the keys being updated */
        final SizedLongChunk<OrderedRowKeys> keyChunk;

        /** A Chunk of longs to store the keys being updated after a permutation */
        final SizedLongChunk<RowKeys> permutedKeyChunk;

        /** A chunk to store the bucket position of individual values in the key and value chunks */
        final SizedIntChunk<RowKeys> outputPositions;

        /** A chunk to store the start positions of runs after a run detection and possible permutation */
        final SizedIntChunk<ChunkPositions> runStarts;

        /**
         * A chunk to store the lengths of runs after a run detection and possible permutation. Parallel to runStarts
         */
        final SizedIntChunk<ChunkLengths> runLengths;

        /** A chunk to store the chunk positions of values in the original chunks after a permutation */
        final SizedIntChunk<ChunkPositions> chunkPositions;

        /** The context to use when finding runs of values in buckets within a chunk of data */
        final SizedSafeCloseable<HashedRunFinder.HashedRunContext> findCtx;

        /** The {@link UpdateByStateManager hash table} build context */
        final SafeCloseable bc;

        /** The {@link UpdateByStateManager hash table} probe context */
        SafeCloseable pc;

        /** The index of keys that need to be revisited during the reprocess phase. */
        RowSetBuilderRandom accumulator;

        // TODO: If I write my own I can avoid extra object creation for the pairs and all of the
        // virtual comparator method calls. I will make it work this way first so I'm not chasing
        // data structure bugs.
        /**
         * A Priority queue that we'll use to populate the output position chunk when we are iterating changes by
         * affected bucket.
         */
        TrackerPriorityQueue bucketQ;

        @SuppressWarnings("resource")
        BucketedContext(@NotNull final TableUpdate upstream,
                @NotNull final ModifiedColumnSet keyModifiedColumnSet,
                @Nullable final ModifiedColumnSet[] inputModifiedColumnSets) {
            final int updateSize = UpdateSizeCalculator.chunkSize(upstream, control.chunkCapacityOrDefault());

            this.inputChunkPopulated = new boolean[operators.length];
            this.keysModified = upstream.modifiedColumnSet().containsAny(keyModifiedColumnSet);
            this.chunkSize =
                    UpdateSizeCalculator.chunkSize(updateSize, upstream.shifted(), control.chunkCapacityOrDefault());
            this.opAffected = new boolean[operators.length];
            // noinspection unchecked
            this.fillContexts = new SizedSafeCloseable[operators.length];
            this.opContext = new UpdateByOperator.UpdateContext[operators.length];
            this.keyChunk = new SizedLongChunk<>(chunkSize);
            this.permuteKernels = new PermuteKernel[operators.length];
            this.permutedKeyChunk = new SizedLongChunk<>(chunkSize);

            // noinspection unchecked
            this.postWorkingChunks = new SizedSafeCloseable[operators.length];
            // noinspection unchecked
            this.permutedPostWorkingChunks = new SizedSafeCloseable[operators.length];

            this.outputPositions = new SizedIntChunk<>(chunkSize);
            this.runStarts = new SizedIntChunk<>(chunkSize);
            this.runLengths = new SizedIntChunk<>(chunkSize);
            this.chunkPositions = new SizedIntChunk<>(chunkSize);

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                opAffected[opIdx] = upstream.added().isNonempty() ||
                        upstream.removed().isNonempty() ||
                        upstream.shifted().nonempty() ||
                        (upstream.modifiedColumnSet().nonempty() && (inputModifiedColumnSets == null
                                || upstream.modifiedColumnSet().containsAny(inputModifiedColumnSets[opIdx])));

                // TODO: Would be nice to abort if the thing is unaffected here and let us recreate later on if we need
                // it
                // during reprocess.
                opContext[opIdx] = operators[opIdx].makeUpdateContext(chunkSize);
                if (opAffected[opIdx]) {
                    operators[opIdx].initializeForUpdate(opContext[opIdx], upstream, source.getRowSet(), true, true);
                    anyAffected = true;
                }

                final int slotPosition = inputSourceSlots[opIdx];
                if (fillContexts[slotPosition] == null) {
                    fillContexts[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].makeFillContext(sz, getSharedContext()));
                    fillContexts[slotPosition].ensureCapacity(chunkSize);

                    postWorkingChunks[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].getChunkType().makeWritableChunk(sz));
                    postWorkingChunks[slotPosition].ensureCapacity(chunkSize);

                    // TODO: Maybe can not allocate this all the time.
                    permutedPostWorkingChunks[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].getChunkType().makeWritableChunk(sz));
                    permutedPostWorkingChunks[slotPosition].ensureCapacity(chunkSize);
                    permuteKernels[slotPosition] =
                            PermuteKernel.makePermuteKernel(inputSources[slotPosition].getChunkType());
                }
            }

            findCtx = new SizedSafeCloseable<>(HashedRunFinder.HashedRunContext::new);
            findCtx.ensureCapacity(chunkSize);
            bc = hashTable.makeUpdateByBuildContext(keySources, chunkSize);
        }

        public SharedContext getSharedContext() {
            return sharedContext;
        }

        @Override
        public void close() {
            keyChunk.close();
            permutedKeyChunk.close();
            outputPositions.close();
            runStarts.close();
            runLengths.close();
            chunkPositions.close();
            findCtx.close();
            bc.close();

            if (pc != null) {
                pc.close();
            }

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                opContext[opIdx].close();

                if (fillContexts[opIdx] != null) {
                    fillContexts[opIdx].close();
                }

                if (postWorkingChunks[opIdx] != null) {
                    postWorkingChunks[opIdx].close();
                }

                if (permutedPostWorkingChunks[opIdx] != null) {
                    permutedPostWorkingChunks[opIdx].close();
                }
            }
            sharedContext.close();
        }

        /**
         * Get (and potentially create) the probe context object for use with
         * {@link UpdateByStateManager#remove(SafeCloseable, RowSequence, ColumnSource[], WritableIntChunk)} and
         * {@link UpdateByStateManager#findModifications(SafeCloseable, RowSequence, ColumnSource[], WritableIntChunk)}.
         *
         * @return an appropriate probe context.
         */
        private SafeCloseable getProbeContet() {
            if (pc == null) {
                pc = hashTable.makeUpdateByProbeContext(keySources, chunkSize);
            }

            return pc;
        }

        /**
         * TableUpdate the chunk sizes used for all internal state objects, for use when reprocessing keys is required
         * and the initial selected chunk size is insufficient.
         *
         * @param newChunkSize the new chunk size to use.
         */
        void setChunkSize(final int newChunkSize) {
            if (newChunkSize <= this.chunkSize) {
                return;
            }

            this.chunkSize = newChunkSize;

            // We have to close and recreate the shared context because a .reset() is not enough to ensure that any
            // cached chunks that something stuffed into there are resized.
            this.sharedContext.close();
            this.sharedContext = SharedContext.makeSharedContext();

            keyChunk.ensureCapacity(newChunkSize);
            permutedKeyChunk.ensureCapacity(newChunkSize);
            outputPositions.ensureCapacity(newChunkSize);
            runStarts.ensureCapacity(newChunkSize);
            runLengths.ensureCapacity(newChunkSize);
            chunkPositions.ensureCapacity(newChunkSize);
            findCtx.ensureCapacity(newChunkSize);

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].setChunkSize(opContext[opIdx], newChunkSize);
                    if (fillContexts[opIdx] != null) {
                        fillContexts[opIdx].ensureCapacity(newChunkSize);
                        postWorkingChunks[opIdx].ensureCapacity(newChunkSize);
                        permutedPostWorkingChunks[opIdx].ensureCapacity(newChunkSize);
                    }
                }
            }
        }

        /**
         * Ensure that all operators have enough space to support the proper number of buckets.
         *
         * @param nBuckets the number of buckets
         */
        public void setBucketCapacity(final int nBuckets) {
            for (final UpdateByOperator op : operators) {
                op.setBucketCapacity(nBuckets);
            }
        }

        /**
         * Notify all operators that some buckets were removed entirely.
         *
         * @param removedBuckets the buckets that were removed.
         */
        private void onBucketsRemoved(@NotNull final RowSet removedBuckets) {
            for (final UpdateByOperator operator : operators) {
                operator.onBucketsRemoved(removedBuckets);
            }
        }

        /**
         * Mark all columns as affected
         */
        public void setAllAffected() {
            Arrays.fill(opAffected, true);
        }

        /**
         * Check if any of the operators produced modifications beyond the set specified by the upstream udpate.
         *
         * @return true if additional updates were produced.
         */
        boolean anyModified() {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx] && operators[opIdx].anyModified(opContext[opIdx])) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Initialize the operators for the specified operation. This is always going to be add or reprocess.
         *
         * @param updateIndex the index that will be processed
         * @param type the type of update to prepare for.
         */
        void initializeFor(@NotNull final RowSet updateIndex,
                @NotNull final UpdateType type) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].initializeFor(opContext[opIdx], updateIndex, type);
                }
            }
        }

        /**
         * Finish the operation specified. This will always be preceded by a single call to
         * {@link #initializeFor(RowSet, UpdateType)}
         *
         * @param type the type
         */
        void finishFor(@NotNull final UpdateType type) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishFor(opContext[opIdx], type);
                }
            }
        }

        /**
         * Prepare the specified chunk for use.
         *
         * @param inputSlot the slot containing the input source
         * @param permuteRequired if a permutation of the copied values is required
         * @param chunkOk the chunk of keys to read
         * @param workingChunk the working chunk of values to fill
         * @param chunkPositions the positions within the original chunk -- used when permuting values
         * @param permutedWorkingChunk the permuted result chunk, if permutation was required.
         * @param fillContext the {@link ChunkSource.FillContext}
         */
        private void prepareValuesChunkFor(final int inputSlot,
                final boolean permuteRequired,
                final RowSequence chunkOk,
                final WritableChunk<Values> workingChunk,
                final IntChunk<ChunkPositions> chunkPositions,
                final WritableChunk<? super Values> permutedWorkingChunk,
                final ChunkSource.FillContext fillContext) {
            if (!inputChunkPopulated[inputSlot]) {
                inputChunkPopulated[inputSlot] = true;
                inputSources[inputSlot].fillChunk(fillContext, workingChunk, chunkOk);
                if (permuteRequired) {
                    permuteKernels[inputSlot].permuteInput(workingChunk,
                            chunkPositions,
                            permutedWorkingChunk);
                }
            }
        }

        /**
         * Process the shifts and modifies of the upstream and separate rows that had key changes from those that did
         * not. For rows that did, treat them as removed from one bucket and added to another. This method is taken from
         * {@link ChunkedOperatorAggregationHelper} and modified to suit the needs of updateBy.
         */
        private void processModifiesForChangedKeys(@NotNull final TableUpdate upstream) {
            Require.requirement(keysModified, "keysModified");

            final boolean shifted = upstream.shifted().nonempty();
            try (final RowSequence.Iterator modifiedPreShiftIterator =
                    upstream.getModifiedPreShift().getRowSequenceIterator();
                    final RowSequence.Iterator modifiedPostShiftIterator =
                            shifted ? upstream.modified().getRowSequenceIterator() : null;
                    final WritableIntChunk<RowKeys> postSlots = WritableIntChunk.makeWritableChunk(chunkSize)) {

                // NULL_ROW_KEY is lower than any other possible index, so we should not trip on the first pass
                long lastCurrentIndex = NULL_ROW_KEY;
                long lastPreviousIndex = NULL_ROW_KEY;

                final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();

                // We will repurpose keyChunk and permutedKeyChunk here to avoid creating even more chunks
                final WritableLongChunk<OrderedRowKeys> preShiftIndicesChunk = keyChunk.get();
                final WritableLongChunk<OrderedRowKeys> postShiftIndicesChunk =
                        WritableLongChunk.downcast(permutedKeyChunk.get());
                while (modifiedPreShiftIterator.hasMore()) {
                    final RowSequence modifiedPreShiftOk =
                            modifiedPreShiftIterator.getNextRowSequenceWithLength(chunkSize);
                    final RowSequence modifiedPostShiftOk =
                            shifted ? modifiedPostShiftIterator.getNextRowSequenceWithLength(chunkSize)
                                    : modifiedPreShiftOk;

                    hashTable.remove(getProbeContet(), modifiedPreShiftOk, keySources, localOutputPositions);
                    hashTable.add(false, bc, modifiedPostShiftOk, keySources, nextOutputPosition, postSlots);

                    modifiedPreShiftOk.fillRowKeyChunk(preShiftIndicesChunk);
                    final LongChunk<OrderedRowKeys> postShiftIndices;
                    if (shifted) {
                        modifiedPostShiftOk.fillRowKeyChunk(postShiftIndicesChunk);
                        postShiftIndices = postShiftIndicesChunk;
                    } else {
                        postShiftIndices = preShiftIndicesChunk;
                    }

                    final int chunkSize = localOutputPositions.size();
                    Assert.eq(chunkSize, "postSlots.size()", postSlots.size(), "postSlots.size()");
                    Assert.eq(chunkSize, "chunkSize", preShiftIndicesChunk.size(), "preShiftIndices.size()");
                    Assert.eq(chunkSize, "chunkSize", postShiftIndices.size(), "postShiftIndices.size()");
                    for (int si = 0; si < chunkSize; ++si) {
                        final int previousSlot = localOutputPositions.get(si);
                        final int currentSlot = postSlots.get(si);
                        final long previousIndex = preShiftIndicesChunk.get(si);
                        final long currentIndex = postShiftIndices.get(si);

                        Assert.gt(currentIndex, "currentIndex", lastCurrentIndex, "lastCurrentIndex");
                        Assert.gt(previousIndex, "previousIndex", lastPreviousIndex, "lastPreviousIndex");

                        // TODO: Would it be better to do this in chunks? This method requires:
                        // 1) LCS get()
                        // 2) comparison
                        // 3) subtraction
                        // 4) LCS get()
                        // 5) randombuilder insert
                        //
                        // Using chunks would require
                        // 1) 2 more chunks allocated
                        // 2) Hashed run finder x2 (one for modified only, one for removed)
                        // 3) permute x2
                        if (previousSlot == currentSlot) {
                            slotTracker.modifyBucket(currentSlot, currentIndex);
                        } else {
                            slotTracker.removeFromBucket(previousSlot, previousIndex);
                            slotTracker.addToBucket(currentSlot, currentIndex);
                        }

                        lastCurrentIndex = currentIndex;
                        lastPreviousIndex = previousIndex;
                    }
                }
            }
        }

        /**
         * Process the upstream additions and accumulate them to the {@link UpdateBySlotTracker}
         *
         * @param additions the additions.
         */
        private void accumulateAdditions(@NotNull final RowSequence additions) {
            try (final RowSequence.Iterator okIt = additions.getRowSequenceIterator()) {
                final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();
                final WritableIntChunk<ChunkPositions> localRunStarts = runStarts.get();
                final WritableIntChunk<ChunkLengths> localRunLengths = runLengths.get();
                final WritableLongChunk<RowKeys> localPermutedKeyIndicesChunk = permutedKeyChunk.get();
                final WritableLongChunk<OrderedRowKeys> localKeyIndicesChunk = keyChunk.get();
                while (okIt.hasMore()) {
                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);
                    hashTable.add(false, bc, chunkOk, keySources, nextOutputPosition, localOutputPositions);
                    final boolean permuteRequired = findRunsAndPermute(chunkOk);

                    for (int runIdx = 0; runIdx < localRunStarts.size(); runIdx++) {
                        final int runStart = localRunStarts.get(runIdx);
                        final int runLength = localRunLengths.get(runIdx);
                        final int bucketPosition = localOutputPositions.get(runStart);

                        slotTracker.addToBucket(bucketPosition,
                                permuteRequired ? localPermutedKeyIndicesChunk : localKeyIndicesChunk,
                                runStart,
                                runLength);
                    }
                }
            }
        }

        /**
         * Process the upstream removals and accumulate them to the {@link UpdateBySlotTracker}
         *
         * @param removals the removals.
         */
        private void accumulateRemovals(@NotNull final RowSequence removals) {
            try (final RowSequence.Iterator okIt = removals.getRowSequenceIterator()) {
                final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();
                final WritableIntChunk<ChunkPositions> localRunStarts = runStarts.get();
                final WritableIntChunk<ChunkLengths> localRunLengths = runLengths.get();
                final WritableLongChunk<RowKeys> localPermutedKeyIndicesChunk = permutedKeyChunk.get();
                final WritableLongChunk<OrderedRowKeys> localKeyIndicesChunk = keyChunk.get();
                while (okIt.hasMore()) {
                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);
                    hashTable.remove(getProbeContet(), chunkOk, keySources, localOutputPositions);
                    final boolean permuteRequired = findRunsAndPermute(chunkOk);

                    for (int runIdx = 0; runIdx < localRunStarts.size(); runIdx++) {
                        final int runStart = localRunStarts.get(runIdx);
                        final int runLength = localRunLengths.get(runIdx);
                        final int bucketPosition = localOutputPositions.get(runStart);

                        slotTracker.removeFromBucket(bucketPosition,
                                permuteRequired ? localPermutedKeyIndicesChunk : localKeyIndicesChunk,
                                runStart,
                                runLength);
                    }
                }
            }
        }

        /**
         * Process the upstream modifications and accumulate them to the {@link UpdateBySlotTracker}. Note that this
         * method can only be called if there are no changes to the key columns. Otherwise we must use
         * {@link #processModifiesForChangedKeys(TableUpdate)}.
         *
         * @param modifications the the modifications.
         */
        private void accumulateModifications(@NotNull final RowSequence modifications) {
            try (final RowSequence.Iterator okIt = modifications.getRowSequenceIterator()) {
                final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();
                final WritableIntChunk<ChunkPositions> localRunStarts = runStarts.get();
                final WritableIntChunk<ChunkLengths> localRunLengths = runLengths.get();
                final WritableLongChunk<RowKeys> localPermutedKeyIndicesChunk = permutedKeyChunk.get();
                final WritableLongChunk<OrderedRowKeys> localKeyIndicesChunk = keyChunk.get();
                while (okIt.hasMore()) {
                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);
                    hashTable.findModifications(getProbeContet(), chunkOk, keySources, localOutputPositions);
                    final boolean permuteRequired = findRunsAndPermute(chunkOk);

                    for (int runIdx = 0; runIdx < localRunStarts.size(); runIdx++) {
                        final int runStart = localRunStarts.get(runIdx);
                        final int runLength = localRunLengths.get(runIdx);
                        final int bucketPosition = localOutputPositions.get(runStart);

                        slotTracker.modifyBucket(bucketPosition,
                                permuteRequired ? localPermutedKeyIndicesChunk : localKeyIndicesChunk,
                                runStart,
                                runLength);
                    }
                }
            }
        }

        /**
         * Proces the upstream shifts and determine which buckets are affected by the shifts.
         *
         * @param shifted the upstream shifts.
         */
        public void accumulateShifts(@NotNull final RowSetShiftData shifted) {
            final WritableLongChunk<OrderedRowKeys> localKeyChunk = keyChunk.get();
            int nextChunkPosition = 0;

            // We don't want to apply shifts later on and be linear in buckets, so let's identify the affected shift
            // buckets
            // and only apply to those.
            try (final RowSet.SearchIterator curIt = source.getRowSet().searchIterator()) {
                for (int shiftIdx = 0; shiftIdx < shifted.size(); shiftIdx++) {
                    final long start = shifted.getBeginRange(shiftIdx);
                    final long end = shifted.getEndRange(shiftIdx);
                    final long delta = shifted.getShiftDelta(shiftIdx);

                    // If we don't have any more valid keys, we're done.
                    if (!curIt.advance(start + delta)) {
                        break;
                    }

                    // Accumulate the keys into a chunk until it is full,
                    // Then we'll hash the chunk to bucket it and accumulate those into the tracker.
                    long keyToTrack = curIt.currentValue();
                    while (keyToTrack <= (end + delta)) {
                        localKeyChunk.set(nextChunkPosition++, keyToTrack);
                        if (nextChunkPosition == localKeyChunk.capacity()) {
                            localKeyChunk.setSize(nextChunkPosition);
                            locateAndTrackShiftedBuckets();
                            nextChunkPosition = 0;
                        }

                        if (!curIt.hasNext()) {
                            break;
                        }
                        keyToTrack = curIt.nextLong();
                    }
                }

                // Make sure we hash whatever is left over.
                if (nextChunkPosition > 0) {
                    localKeyChunk.setSize(nextChunkPosition);
                    locateAndTrackShiftedBuckets();
                }
            }
        }

        /**
         * Hash the output positions chunk to determine which buckets are affected by the shifts, then pass that
         * information along to the slot tracker,
         */
        private void locateAndTrackShiftedBuckets() {
            final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();
            final WritableIntChunk<ChunkPositions> localRunStarts = runStarts.get();
            hashTable.findModifications(getProbeContet(),
                    RowSequenceFactory.wrapRowKeysChunkAsRowSequence(keyChunk.get()),
                    keySources,
                    localOutputPositions);

            HashedRunFinder.findRunsHashed(findCtx.get(),
                    localRunStarts,
                    runLengths.get(),
                    chunkPositions.get(),
                    localOutputPositions);

            for (int runIdx = 0; runIdx < localRunStarts.size(); runIdx++) {
                final int runStart = localRunStarts.get(runIdx);
                final int bucketPosition = localOutputPositions.get(runStart);
                slotTracker.markForShift(bucketPosition);
            }
        }

        /**
         * Accumulate all the bucketed changes into an index to reprocess. If we can detect that a particular bucket was
         * simply appended to, we can just accumulate the "added part of that bucket's TableUpdate, otherwise we
         * determine the smallest affected key for each bucket and insert a sub-index of the bucket's total index
         * beginning at that lowest key. This produces a reprocessing index that we can iterate which will only contain
         * rows from buckets that were actually affected, yet still allows us to reprocess those rows sequentially.
         *
         * @param tracker the tracker object for this slot.
         * @param slotIndex the index for this slot, after the specified updates have been
         *        {@link UpdateBySlotTracker#applyUpdates(RowSetShiftData) applied}
         */
        private void accumulateIndexToReprocess(@NotNull final UpdateBySlotTracker.UpdateTracker tracker,
                @NotNull final RowSet slotIndex) {
            if (tracker.wasShiftOnly()) {
                // If the bucket was simply shifted, there's nothing to do. None of the data changed, and the output
                // sources
                // will get adjusted when we call the shiftOutputSources() method later.
                return;
            }

            final RowSet indexToAccumulate = tracker.wasAppendOnly() ? tracker.getAdded() : slotIndex;
            final long smallestModifiedKey = tracker.getSmallestModifiedKey();
            boolean addedRowsToReprocess = false;

            // This will never be less than, but it costs us nothing.
            if (smallestModifiedKey <= slotIndex.firstRowKey()) {
                addedRowsToReprocess = true;
                accumulator.addRowSet(slotIndex);
            } else if (smallestModifiedKey <= slotIndex.lastRowKey()) {
                addedRowsToReprocess = true;
                try (final RowSet indexToInsert =
                        indexToAccumulate.subSetByKeyRange(smallestModifiedKey, indexToAccumulate.lastRowKey())) {
                    accumulator.addRowSet(indexToInsert);
                }
            }

            // It's possible we marked the operator as untouched if it's column was not affected. Then, later on,
            // we discovered that a key changed and a row switched buckets. That means one bucket had removed rows
            // and the other had added rows, and now the operators -need- to be visited.
            final boolean bucketHasAddedOrRemoved =
                    tracker.getAdded().isNonempty() || tracker.getRemoved().isNonempty();

            // If the tracker wasn't append only, we'll issue a reset command to the operators for this bucket.
            if (!tracker.wasAppendOnly()) {
                final long keyBefore;
                try (final RowSet.SearchIterator sit = slotIndex.searchIterator()) {
                    keyBefore = sit.binarySearchValue(
                            (compareTo, ignored) -> Long.compare(smallestModifiedKey - 1, compareTo), 1);
                }

                // Notify the operators to reset their bucket states, and update their affectivity.
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    opAffected[opIdx] |= bucketHasAddedOrRemoved;
                    if (opAffected[opIdx]) {
                        operators[opIdx].resetForReprocess(opContext[opIdx], slotIndex, tracker.getSlot(), keyBefore);
                    }
                }
            }

            if (!addedRowsToReprocess) {
                // There's nothing to do, the modified key is after our last valid index.
                return;
            }

            final RowSet.SearchIterator iterator = slotIndex.searchIterator();
            iterator.advance(smallestModifiedKey);
            tracker.setBucketIterator(iterator);

            if (bucketQ == null) {
                bucketQ = new TrackerPriorityQueue((int) slotTracker.getModifiedBucketCount());
            }
            bucketQ.add(tracker);
        }

        /**
         * Iterate over the set of rows accumulated from the set of modified buckets by
         * {@link #accumulateIndexToReprocess(UpdateBySlotTracker.UpdateTracker, RowSet)} This will use the
         * {@link #bucketQ priority queue} to associate values with buckets, instead of the hash table and then push the
         * update down to the operators.
         */
        public void processBucketedUpdates() {
            final RowSet modifiedBucketIndex = accumulator.build();

            // This can happen if all we did was completely remove some buckets.
            if (modifiedBucketIndex.isEmpty()) {
                return;
            }

            try (final RowSequence.Iterator okIt = modifiedBucketIndex.getRowSequenceIterator()) {
                final int newChunkSize = (int) Math.min(control.chunkCapacityOrDefault(), modifiedBucketIndex.size());
                setChunkSize(newChunkSize);
                initializeFor(modifiedBucketIndex, UpdateType.Reprocess);

                // TODO: This is a lot of iteration. Maybe a RangeIterator would be more better for the buckets?
                UpdateBySlotTracker.UpdateTracker tracker = bucketQ.pop();
                if (tracker == null) {
                    Assert.statementNeverExecuted("Bucketing queue is empty, but there are still keys to process");
                }
                RowSet.SearchIterator trackerIt = tracker.getIterator();
                long trackerCurKey = tracker.getIterator().currentValue();

                final WritableLongChunk<OrderedRowKeys> localKeyChunk = keyChunk.get();
                final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();
                final WritableIntChunk<ChunkPositions> localRunStarts = runStarts.get();
                final WritableIntChunk<ChunkLengths> localRunLengths = runLengths.get();
                final WritableIntChunk<ChunkPositions> localChunkPositions = chunkPositions.get();
                final WritableLongChunk<RowKeys> localPermutedKeyChunk = permutedKeyChunk.get();

                while (okIt.hasMore()) {
                    sharedContext.reset();
                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);
                    chunkOk.fillRowKeyChunk(localKeyChunk);

                    // TableUpdate the outputPositions chunk using the PriQ we built during the accumulate step
                    for (int chunkPosition = 0; chunkPosition < localKeyChunk.size(); chunkPosition++) {
                        final long keyToInspect = localKeyChunk.get(chunkPosition);
                        // If the key we're looking for is not part of this bucket, insert it back into
                        // the queue, so long as there is a next key.
                        if (keyToInspect != trackerCurKey) {
                            if (trackerCurKey != NULL_ROW_KEY) {
                                bucketQ.add(tracker);
                            }

                            // Grab the next bucket from the queue and continue.
                            tracker = bucketQ.pop();
                            if (tracker == null) {
                                Assert.statementNeverExecuted(
                                        "Bucketing queue is empty, but there are still keys to process");
                            }
                            trackerIt = tracker.getIterator();
                            trackerCurKey = trackerIt.currentValue();
                            Assert.eq(trackerCurKey, "trackerCurKey", keyToInspect, "keyToInspect");
                        }

                        // Mark this position with the current bucket's position.
                        localOutputPositions.set(chunkPosition, tracker.getSlot());
                        // Advance the iterator, using NULL_ROW_KEY if there are no further keys.
                        trackerCurKey = trackerIt.hasNext() ? trackerIt.nextLong() : NULL_ROW_KEY;
                    }
                    localOutputPositions.setSize(chunkOk.intSize());

                    // Now, find the runs of buckets and permute them if necessary
                    final boolean permuteRequired = HashedRunFinder.findRunsHashed(findCtx.get(),
                            localRunStarts,
                            localRunLengths,
                            localChunkPositions,
                            localOutputPositions);

                    // Now permute the keys to match the found ranges
                    if (permuteRequired) {
                        localPermutedKeyChunk.setSize(localKeyChunk.size());
                        LongPermuteKernel.permuteInput(localKeyChunk, localChunkPositions, localPermutedKeyChunk);
                    }

                    Arrays.fill(inputChunkPopulated, false);
                    // Finally, push the chunk down into the affected oeprators.
                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (opAffected[opIdx]) {
                            final int slotPosition = inputSourceSlots[opIdx];
                            final WritableChunk<Values> localPostWorkingChunk = postWorkingChunks[slotPosition].get();
                            final WritableChunk<Values> localPermutedWorkingChunk =
                                    permutedPostWorkingChunks[slotPosition].get();

                            prepareValuesChunkFor(
                                    slotPosition,
                                    permuteRequired,
                                    chunkOk,
                                    localPostWorkingChunk,
                                    localChunkPositions,
                                    localPermutedWorkingChunk,
                                    fillContexts[slotPosition].get());

                            operators[opIdx].reprocessChunk(opContext[opIdx],
                                    chunkOk,
                                    permuteRequired ? localPermutedWorkingChunk : localPostWorkingChunk,
                                    permuteRequired ? localPermutedKeyChunk : localKeyChunk,
                                    localOutputPositions,
                                    localRunStarts,
                                    localRunLengths);
                        }
                    }
                }
            }

            finishFor(UpdateType.Reprocess);
        }

        /**
         * Inspect the chunk positions and find sorted runs of individual buckets. If required, permute the input key
         * chunk so that input keys remain adjacent to their values.
         *
         * @param chunkOk the chunk of input keys.
         */
        private boolean findRunsAndPermute(@NotNull final RowSequence chunkOk) {
            final boolean permuteRequired = HashedRunFinder.findRunsHashed(findCtx.get(),
                    runStarts.get(),
                    runLengths.get(),
                    chunkPositions.get(),
                    outputPositions.get());

            chunkOk.fillRowKeyChunk(keyChunk.get());
            // Now permute the keys to match the found ranges
            if (permuteRequired) {
                permutedKeyChunk.get().setSize(chunkOk.intSize());
                LongPermuteKernel.permuteInput(keyChunk.get(), chunkPositions.get(), permutedKeyChunk.get());
            }

            return permuteRequired;
        }

        /**
         * Push the specified index as an append-only update to the operators.
         *
         * @param added the keys added.
         */
        private void doAppendOnlyAdds(final boolean initialBuild, @NotNull final RowSet added) {
            initializeFor(added, UpdateType.Add);

            try (final RowSequence.Iterator okIt = added.getRowSequenceIterator()) {
                final WritableIntChunk<ChunkPositions> localRunStarts = runStarts.get();
                final WritableIntChunk<ChunkLengths> localRunLengths = runLengths.get();
                final WritableIntChunk<RowKeys> localOutputPositions = outputPositions.get();
                final WritableLongChunk<RowKeys> localPermutedKeys = permutedKeyChunk.get();
                final WritableLongChunk<OrderedRowKeys> localKeys = keyChunk.get();
                final WritableIntChunk<ChunkPositions> localChunkPositions = chunkPositions.get();

                while (okIt.hasMore()) {
                    sharedContext.reset();
                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);

                    // add the values to the hash table, and produce a chunk of the positions each added key mapped to
                    hashTable.add(initialBuild, bc, chunkOk, keySources, nextOutputPosition, localOutputPositions);
                    setBucketCapacity(nextOutputPosition.intValue());

                    // Now, organize that chunk by position so we can hand them off to the operators
                    final boolean permuteRequired = findRunsAndPermute(chunkOk);

                    // If we need to track the slot indices, make sure we insert them into the tracker.
                    if (slotTracker != null) {
                        for (int runIdx = 0; runIdx < localRunStarts.size(); runIdx++) {
                            final int runStart = localRunStarts.get(runIdx);
                            final int runLength = localRunLengths.get(runIdx);
                            final int bucketPosition = localOutputPositions.get(runStart);

                            slotTracker.addToBucket(bucketPosition,
                                    permuteRequired ? localPermutedKeys : localKeys,
                                    runStart,
                                    runLength);
                        }
                    }

                    Arrays.fill(inputChunkPopulated, false);
                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (opAffected[opIdx]) {
                            final int slotPosition = inputSourceSlots[opIdx];
                            final WritableChunk<Values> localPostWorkingChunk = postWorkingChunks[slotPosition].get();
                            final WritableChunk<Values> localPermutedPostWorkingChunk =
                                    permutedPostWorkingChunks[slotPosition].get();
                            prepareValuesChunkFor(
                                    slotPosition,
                                    permuteRequired,
                                    chunkOk,
                                    localPostWorkingChunk,
                                    localChunkPositions,
                                    localPermutedPostWorkingChunk,
                                    fillContexts[slotPosition].get());

                            operators[opIdx].addChunk(opContext[opIdx],
                                    permuteRequired ? localPermutedPostWorkingChunk : localPostWorkingChunk,
                                    permuteRequired ? localPermutedKeys : localKeys,
                                    localOutputPositions,
                                    localRunStarts,
                                    localRunLengths);
                        }
                    }
                }
            }

            finishFor(UpdateType.Add);
        }

        private void applyShiftsToOutput(@NotNull final RowSetShiftData shifts) {
            if (shifts.empty()) {
                return;
            }

            try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                shifts.apply((begin, end, delta) -> {
                    try (final RowSet subIndex = prevIdx.subSetByKeyRange(begin, end)) {
                        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                            operators[opIdx].applyOutputShift(opContext[opIdx], subIndex, delta);
                        }
                    }
                });
            }
        }
    }

    /**
     * An object to hold the transient state during a static grouped bucketed addition
     */
    private class GroupedContext implements SafeCloseable {
        /** An array of closeables to help with cleanup */
        final SafeCloseableList closeables = new SafeCloseableList();

        /** The expected size of chunks to the various update stages */
        final int chunkSize;

        /** An indicator of if each slot has been populated with data or not for this phase. */
        boolean[] inputChunkPopulated;

        /** An array of context objects for each underlying operator */
        final UpdateByOperator.UpdateContext[] opContext;

        /** A {@link SharedContext} to be used while creating other contexts */
        final SharedContext sharedContext = SharedContext.makeSharedContext();

        /** An array of {@link ChunkSource.FillContext}s for each input column */
        final ChunkSource.FillContext[] fillContexts;

        /** A set of chunks used to store post-shift working values */
        final WritableChunk<Values>[] postWorkingChunks;

        GroupedContext(final TableUpdate upstream) {
            this.chunkSize = Math.min((int) source.size(), control.chunkCapacityOrDefault());
            this.inputChunkPopulated = new boolean[operators.length];
            this.fillContexts = new ChunkSource.FillContext[operators.length];
            this.opContext = new UpdateByOperator.UpdateContext[operators.length];

            // noinspection unchecked
            this.postWorkingChunks = new WritableChunk[operators.length];

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                opContext[opIdx] = closeables.add(operators[opIdx].makeUpdateContext(chunkSize));
                final int slotPosition = inputSourceSlots[opIdx];
                if (fillContexts[slotPosition] == null) {
                    fillContexts[slotPosition] =
                            closeables.add(inputSources[slotPosition].makeFillContext(chunkSize, sharedContext));
                    postWorkingChunks[slotPosition] =
                            closeables.add(inputSources[slotPosition].getChunkType().makeWritableChunk(chunkSize));
                }
                operators[opIdx].initializeForUpdate(opContext[opIdx], upstream, source.getRowSet(), false, true);
            }
        }

        void initialize(@NotNull final RowSet bucketIndex) {
            sharedContext.reset();
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                operators[opIdx].initializeFor(opContext[opIdx], bucketIndex, UpdateType.Add);
            }
        }

        void finish() {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                operators[opIdx].finishFor(opContext[opIdx], UpdateType.Add);
            }
        }

        @Override
        @OverridingMethodsMustInvokeSuper
        public void close() {
            closeables.close();
            sharedContext.close();
        }

        /**
         * Prepare the specified chunk for use.
         *
         * @param inputSlot the slot holding the input source and contexts
         * @param workingChunk the working chunk of values to fill
         * @param fillContext the {@link ChunkSource.FillContext}
         * @param chunkOk the {@link RowSequence} for current values
         */
        private void prepareValuesChunkFor(final int inputSlot,
                final RowSequence chunkOk,
                final WritableChunk<Values> workingChunk,
                final ChunkSource.FillContext fillContext) {
            if (!inputChunkPopulated[inputSlot]) {
                inputChunkPopulated[inputSlot] = true;
                inputSources[inputSlot].fillChunk(fillContext, workingChunk, chunkOk);
            }
        }
    }

    /**
     * Perform a bucketed updateBy.
     *
     * @param control the {@link UpdateByControl} to set hashing parameters
     * @param description the operation description
     * @param source the source table
     * @param ops the operations to perform
     * @param resultSources the result sources
     * @param rowRedirection the redirection index, if one was used.
     * @param keySources the sources for key columns.
     * @param byColumns the grouping column pairs
     * @return the result table
     */
    @SuppressWarnings("rawtypes")
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] ops,
            @NotNull final Map<String, ColumnSource<?>> resultSources,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final ColumnSource<?>[] originalKeySources,
            @NotNull final MatchPair[] byColumns,
            @NotNull final UpdateByControl control) {
        final QueryTable result = new QueryTable(source.getRowSet(), resultSources);
        final boolean useGrouping = JoinControl.useGrouping(source, keySources);
        final BucketedUpdateBy updateBy =
                new BucketedUpdateBy(ops, source, keySources, originalKeySources, useGrouping, rowRedirection, control);
        updateBy.doInitialAdditions(useGrouping, byColumns);

        if (source.isRefreshing()) {
            if (rowRedirection != null) {
                rowRedirection.startTrackingPrevValues();
            }
            Arrays.stream(ops).forEach(UpdateByOperator::startTrackingPrev);
            final InstrumentedTableUpdateListener listener = updateBy.newListener(description, result, byColumns);
            source.listenForUpdates(listener);
            result.addParentReference(listener);
        }

        return result;
    }

    private BucketedUpdateBy(@NotNull final UpdateByOperator[] operators,
            @NotNull final QueryTable source,
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final ColumnSource<?>[] originalKeySources,
            final boolean useGrouping,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        super(operators, source, rowRedirection, control);
        this.keySources = keySources;

        final int hashTableSize = control.initialHashTableSizeOrDefault();

        if (source.isRefreshing() && !source.isAddOnly()) {
            slotTracker = new UpdateBySlotTracker(control.chunkCapacityOrDefault());
        } else {
            slotTracker = null;
        }
        if (!useGrouping) {
            this.hashTable = TypedHasherFactory.make(UpdateByStateManagerTypedBase.class,
                    keySources, keySources,
                    hashTableSize, control.maximumLoadFactorOrDefault(),
                    control.targetLoadFactorOrDefault());
        }
    }

    /**
     * Create an appropriate {@link InstrumentedTableUpdateListener listener} to process upstream updates.
     *
     * @param description the table description
     * @param result the result table
     * @param byColumns the grouping column pairs
     * @return a listener to process updates.
     */
    public InstrumentedTableUpdateListener newListener(@NotNull final String description,
            @NotNull final QueryTable result,
            @NotNull final MatchPair[] byColumns) {
        return new BucketedUpdateByListener(description, source, result, byColumns);
    }

    /**
     * Insert the original source table as an add-only operation to initialize the result table.
     *
     * @param useGrouping if grouping should be used.
     * @param byColumns the grouping columns
     */
    private void doInitialAdditions(final boolean useGrouping, @NotNull final MatchPair[] byColumns) {
        if (source.isEmpty()) {
            return;
        }

        final TableUpdateImpl initialUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.ALL);
        if (useGrouping) {
            try (final GroupedContext ctx = new GroupedContext(initialUpdate)) {
                doStaticGroupedAddition(ctx, source.getRowSet());
            }
        } else {
            final ModifiedColumnSet keyModifiedColumnSet =
                    source.newModifiedColumnSet(MatchPair.getRightColumns(byColumns));
            try (final BucketedContext ctx = new BucketedContext(initialUpdate, keyModifiedColumnSet, null)) {
                ctx.setAllAffected();
                ctx.initializeFor(source.getRowSet(), UpdateType.Add);

                if (rowRedirection != null && source.isRefreshing()) {
                    processUpdateForRedirection(initialUpdate);
                }

                ctx.doAppendOnlyAdds(true, source.getRowSet());
                if (slotTracker != null) {
                    // noinspection resource
                    slotTracker.applyUpdates(RowSetShiftData.EMPTY);
                    slotTracker.reset();
                }
            }
        }
    }

    /**
     * Do a bucketed addition using the precomputed grouping data instead of reading the groupBy column.
     *
     * @param ctx the context
     * @param added the index being added
     */
    private void doStaticGroupedAddition(@NotNull final GroupedContext ctx, @NotNull final RowSet added) {

        final Map<?, RowSet> grouping = keySources[0].getGroupToRange(added);

        if (grouping == null) {
            Assert.statementNeverExecuted("Trying to do grouped addition, but no groups exist");
        }

        MutableInt groupPosition = new MutableInt(0);
        grouping.forEach((key, groupRowSet) -> {
            if (groupRowSet == null) {
                Assert.statementNeverExecuted("Found a null RowSet for group key " + key);
            }
            ctx.initialize(groupRowSet);
            try (final RowSequence.Iterator okIt = groupRowSet.getRowSequenceIterator()) {
                while (okIt.hasMore()) {
                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(ctx.chunkSize);
                    final LongChunk<OrderedRowKeys> keyChunk = chunkOk.asRowKeyChunk();
                    Arrays.fill(ctx.inputChunkPopulated, false);
                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        final int slotPosition = inputSourceSlots[opIdx];
                        ctx.prepareValuesChunkFor(
                                slotPosition,
                                chunkOk,
                                ctx.postWorkingChunks[slotPosition],
                                ctx.fillContexts[slotPosition]);

                        operators[opIdx].addChunk(ctx.opContext[opIdx],
                                chunkOk,
                                keyChunk,
                                ctx.postWorkingChunks[slotPosition],
                                groupPosition.getValue());
                    }
                }
            }
            ctx.finish();
            groupPosition.increment();

        });
    }

    class BucketedUpdateByListener extends InstrumentedTableUpdateListenerAdapter {
        private final QueryTable result;

        private final ModifiedColumnSet[] inputModifiedColumnSets;
        private final ModifiedColumnSet[] outputModifiedColumnSets;
        private final ModifiedColumnSet.Transformer transformer;

        private final ModifiedColumnSet keyModifiedColumnSet;

        BucketedUpdateByListener(@Nullable final String description,
                @NotNull final QueryTable source,
                @NotNull final QueryTable result,
                @NotNull final MatchPair[] byColumns) {
            super(description, source, false);
            this.result = result;

            this.inputModifiedColumnSets = new ModifiedColumnSet[operators.length];
            this.outputModifiedColumnSets = new ModifiedColumnSet[operators.length];

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                final String[] outputColumnNames = operators[opIdx].getOutputColumnNames();
                inputModifiedColumnSets[opIdx] =
                        source.newModifiedColumnSet(operators[opIdx].getAffectingColumnNames());
                outputModifiedColumnSets[opIdx] = result.newModifiedColumnSet(outputColumnNames);
            }

            this.keyModifiedColumnSet = source.newModifiedColumnSet(MatchPair.getRightColumns(byColumns));
            this.transformer =
                    source.newModifiedColumnSetTransformer(result, source.getDefinition().getColumnNamesArray());
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            try (final BucketedContext ctx =
                    new BucketedContext(upstream, keyModifiedColumnSet, inputModifiedColumnSets)) {
                if (rowRedirection != null) {
                    processUpdateForRedirection(upstream);
                }

                // If the update was, in itself, append-only, we can just push it through.
                final boolean isAppendOnly =
                        UpdateByOperator.isAppendOnly(upstream, source.getRowSet().lastRowKeyPrev());
                if (isAppendOnly) {
                    ctx.doAppendOnlyAdds(false, upstream.added());
                } else {
                    accumulateUpdatesByBucket(upstream, ctx);
                }

                if (slotTracker != null) {
                    processAccumulatedUpdates(upstream, ctx, isAppendOnly);
                }

                computeDownstreamUpdate(upstream, ctx);
            }
        }

        /**
         * Process the upstream update and divide it into affected buckets, so that we can be smart about how we
         * reprocess rows from buckets. We want to ensure that we can re-read only the rows we need from the table and
         * on top of that, ensure we can read them sequentially without revisiting blocks of data,
         *
         * @param upstream the upstream update
         * @param ctx the update context
         */
        private void accumulateUpdatesByBucket(@NotNull final TableUpdate upstream,
                @NotNull final BucketedContext ctx) {
            // Build a unique TableUpdate for each bucket affected by the
            // modification and issue each of them separately. Hopefully, most buckets
            // will shake out as append-only, while the ones that are not can be marked
            // and revisited during the reprocessing phase.
            if (upstream.removed().isNonempty()) {
                ctx.accumulateRemovals(upstream.removed());
            }

            if (upstream.modified().isNonempty()) {
                if (ctx.keysModified) {
                    ctx.processModifiesForChangedKeys(upstream);
                } else if (ctx.anyAffected) {
                    ctx.accumulateModifications(upstream.modified());
                }
            }

            if (upstream.shifted().nonempty()) {
                ctx.accumulateShifts(upstream.shifted());
            }

            if (upstream.added().isNonempty()) {
                ctx.accumulateAdditions(upstream.added());
            }
        }

        /**
         * Take the updates that have been broken down by bucket and apply them to each bucket sequentially.
         *
         * @param upstream the upstream update
         * @param ctx the update context
         * @param isAppendOnly if the update was append-only
         */
        private void processAccumulatedUpdates(@NotNull final TableUpdate upstream,
                @NotNull final BucketedContext ctx,
                final boolean isAppendOnly) {
            // Apply the updates to the tracked bucket indices and reset the tracker for the
            // next cycle.
            final RowSet emptiedBuckets = slotTracker.applyUpdates(upstream.shifted());
            if (emptiedBuckets.isNonempty()) {
                ctx.onBucketsRemoved(emptiedBuckets);
            }

            if (!isAppendOnly) {
                // First, process the bucketed changes and create an index of rows to be processed.
                // Explicitly, we are not going to let operators decide to handle remove/modify/shift on their own
                // when we are processing general updates like this. Simplifying in this way lets us treat the update
                // as a "reset" to a particular state and then adds. See the method doc for more detail
                ctx.accumulator = RowSetFactory.builderRandom();
                slotTracker.forAllModifiedSlots(ctx::accumulateIndexToReprocess);

                // Note that we must do this AFTER the accumulateIndexToReprocess to ensure that when we reset
                // state, we don't have to worry about shifts messing with our indices. We don't want to do this
                // as a bucket because the shifts have to apply to the output column source, and we'd rather not
                // iterate the shift O(nBuckets * nOps) times, to do it bucket by bucket.
                if (rowRedirection == null) {
                    ctx.applyShiftsToOutput(upstream.shifted());
                }

                ctx.processBucketedUpdates();
            }

            slotTracker.reset();
        }

        /**
         * Compute the downstream update from the result of processing each operator.
         *
         * @param upstream the upstream update.
         * @param ctx the update context
         */
        private void computeDownstreamUpdate(@NotNull final TableUpdate upstream, @NotNull final BucketedContext ctx) {
            final TableUpdateImpl downstream = new TableUpdateImpl();
            // copy these rowSets since TableUpdateImpl#reset will close them with the upstream update
            downstream.added = upstream.added().copy();
            downstream.removed = upstream.removed().copy();
            downstream.shifted = upstream.shifted();

            if (upstream.modified().isNonempty() || ctx.anyModified()) {
                downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                downstream.modifiedColumnSet.clear();

                WritableRowSet modifiedRowSet = RowSetFactory.empty();
                downstream.modified = modifiedRowSet;

                if (upstream.modified().isNonempty()) {
                    // Transform any untouched modified columns to the output.
                    transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                    modifiedRowSet.insert(upstream.modified());
                }

                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (ctx.opAffected[opIdx]) {
                        downstream.modifiedColumnSet.setAll(outputModifiedColumnSets[opIdx]);
                        if (operators[opIdx].anyModified(ctx.opContext[opIdx])) {
                            modifiedRowSet.insert(operators[opIdx].getAdditionalModifications(ctx.opContext[opIdx]));
                        }
                    }
                }

                if (ctx.anyModified()) {
                    modifiedRowSet.remove(upstream.added());
                }
            } else {
                downstream.modified = RowSetFactory.empty();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            }
            result.notifyListeners(downstream);
        }
    }
}
