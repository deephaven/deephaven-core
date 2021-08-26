/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * A chunked, iterative operator that processes indices and/or data from one input column to produce one or more output
 * columns.
 */
public interface IterativeChunkedAggregationOperator {

    IterativeChunkedAggregationOperator[] ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY =
            new IterativeChunkedAggregationOperator[0];

    /**
     * Aggregate a chunk of data into the result columns.
     *
     * @param context the operator-specific context
     * @param values a chunk of values to aggregate
     * @param inputIndices the input indices, in post-shift space
     * @param destinations the destinations in resultColumn to aggregate into, parallel with startPositions and length
     * @param startPositions the starting positions in the chunk for each destination
     * @param length the number of values in the chunk for each destination
     * @param stateModified a boolean output array, parallel to destinations, which is set to true if the corresponding
     *        destination has been modified
     */
    void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified);

    /**
     * Remove a chunk of data previously aggregated into the result columns.
     *
     * @param context the operator-specific context
     * @param values a chunk of values that have been previously aggregated.
     * @param inputIndices the input indices, in pre-shift space
     * @param destinations the destinations in resultColumn to remove the values from, parallel with startPositions and
     *        length
     * @param startPositions the starting positions in the chunk for each destination
     * @param length the number of values in the chunk for each destination
     * @param stateModified a boolean output array, parallel to destinations, which is set to true if the corresponding
     *        destination has been modified
     */
    void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified);

    /**
     * Modify a chunk of data previously aggregated into the result columns using a parallel chunk of new values. Never
     * includes modifies that have been shifted if {@link #requiresIndices()} returns true - those are handled in
     * {@link #shiftChunk(BucketedContext, Chunk, Chunk, LongChunk, LongChunk, IntChunk, IntChunk, IntChunk, WritableBooleanChunk)}.
     *
     * @param context the operator-specific context
     * @param previousValues a chunk of values that have been previously aggregated
     * @param newValues a chunk of values to aggregate
     * @param postShiftIndices the input indices, in post-shift space
     * @param destinations the destinations in resultColumn to remove the values from, parallel with startPositions and
     *        length
     * @param startPositions the starting positions in the chunk for each destination
     * @param length the number of values in the chunk for each destination
     * @param stateModified a boolean output array, parallel to destinations, which is set to true if the corresponding
     *        destination has been modified
     */
    default void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        try (final WritableBooleanChunk<Values> addModified =
                WritableBooleanChunk.makeWritableChunk(stateModified.size())) {
            // There are no shifted indices here for any operators that care about indices, hence it is safe to remove
            // in "post-shift" space.
            removeChunk(context, previousValues, postShiftIndices, destinations, startPositions, length, stateModified);
            addChunk(context, newValues, postShiftIndices, destinations, startPositions, length, addModified);
            for (int ii = 0; ii < stateModified.size(); ++ii) {
                stateModified.set(ii, stateModified.get(ii) || addModified.get(ii));
            }
        }
    }

    /**
     * Called with shifted indices when {@link #requiresIndices()} returns true, including shifted same-slot modifies.
     *
     * @param context the operator-specific context
     * @param previousValues a chunk of values that have been previously aggregated.
     * @param newValues a chunk of values to aggregate
     * @param preShiftIndices the input indices, in pre-shift space
     * @param postShiftIndices the input indices, in post-shift space
     * @param destinations the destinations in resultColumn to aggregate into, parallel with startPositions and length
     * @param startPositions the starting positions in the chunk for each destination
     * @param length the number of values in the chunk for each destination
     * @param stateModified a boolean output array, parallel to destinations, which is set to true if the corresponding
     *        destination has been modified
     */
    default void shiftChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends KeyIndices> preShiftIndices, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // we don't actually care
    }

    /**
     * Called with the modified indices when {@link #requiresIndices()} returns true if our input columns have not
     * changed (or we have none).
     *
     * @param context the operator-specific context
     * @param inputIndices the input indices, in post-shift space
     * @param destinations the destinations in resultColumn to aggregate into, parallel with startPositions and length
     * @param startPositions the starting positions in the chunk for each destination
     * @param length the number of values in the chunk for each destination
     * @param stateModified a boolean output array, parallel to destinations, which is set to true if the corresponding
     *        destination has been modified
     */
    default void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // we don't actually care
    }

    /**
     * Aggregate a chunk of data into the result columns.
     *
     * @param context the operator-specific context
     * @param chunkSize the size of the addition
     * @param values the values to aggregate
     * @param inputIndices the input indices, in post-shift space
     * @param destination the destination in the result columns
     * @return true if the state was modified, false otherwise
     */
    boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination);

    /**
     * Remove a chunk of data previously aggregated into the result columns.
     *
     * @param context the operator-specific context
     * @param chunkSize the size of the removal
     * @param values the values to remove from the aggregation
     * @param inputIndices the input indices, in pre-shift space
     * @param destination the destination in the result columns
     * @return true if the state was modified, false otherwise
     */
    boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination);

    /**
     * Modify a chunk of data previously aggregated into the result columns using a parallel chunk of new values. Never
     * includes modifies that have been shifted if {@link #requiresIndices()} returns true - those are handled in
     * {@link #shiftChunk(SingletonContext, Chunk, Chunk, LongChunk, LongChunk, long)}.
     *
     * @param context the operator-specific context
     * @param chunkSize the size of the modification
     * @param previousValues a chunk of values that have been previously aggregated.
     * @param newValues a chunk of values to aggregate
     * @param postShiftIndices the input indices, in post-shift space
     * @return true if the state was modified, false otherwise
     */
    default boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        // There are no shifted indices here for any operators that care about indices, hence it is safe to remove in
        // "post-shift" space.
        final boolean modifiedOld = removeChunk(context, chunkSize, previousValues, postShiftIndices, destination);
        final boolean modifiedNew = addChunk(context, chunkSize, newValues, postShiftIndices, destination);
        return modifiedOld || modifiedNew;
    }

    /**
     * Shift a chunk of data previously aggregated into the result columns, including shifted same-slot modifies..
     *
     * @param context the operator-specific context
     * @param previousValues a chunk of values that have been previously aggregated.
     * @param newValues a chunk of values to aggregate
     * @param preShiftIndices the input indices, in pre-shift space
     * @param postShiftIndices the input indices, in post-shift space
     * @param destination the destination in the result columns
     * @return true if the result should be considered modified
     */
    default boolean shiftChunk(SingletonContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends KeyIndices> preShiftIndices, LongChunk<? extends KeyIndices> postShiftIndices,
            long destination) {
        // we don't actually care
        return false;
    }

    /**
     * Called with the modified indices when {@link #requiresIndices()} returns true if our input columns have not
     * changed (or we have none).
     *
     * @param context the operator-specific context
     * @param indices the modified indices for a given destination, in post-shift space
     * @param destination the destination that was modified
     * @return true if the result should be considered modified
     */
    default boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices, long destination) {
        return false;
    }

    /**
     * Whether the operator requires indices. This implies that the operator must process shifts (i.e.
     * {@link #shiftChunk}), and must observe modifications even when its input columns (if any) are not modified (i.e.
     * {@link #modifyIndices}).
     *
     * @return true if the operator requires indices, false otherwise
     */
    default boolean requiresIndices() {
        return false;
    }


    /**
     * Whether the operator can deal with an unchunked Index more efficiently than a chunked index.
     *
     * @return true if the operator can deal with unchunked indices, false otherwise
     */
    default boolean unchunkedIndex() {
        return false;
    }

    default boolean addIndex(SingletonContext context, Index index, long destination) {
        throw new UnsupportedOperationException();
    }

    /**
     * Ensure that this operator can handle destinations up to tableSize - 1.
     *
     * @param tableSize the new size of the table
     */
    void ensureCapacity(long tableSize);

    /**
     * Return a map of result columns produced by this operator.
     *
     * @return a map of name to columns for the result table
     */
    Map<String, ? extends ColumnSource<?>> getResultColumns();

    /**
     * Perform any internal state keeping needed for destinations that were added during initialization.
     *
     * @param resultTable The result {@link QueryTable} after initialization
     */
    default void propagateInitialState(@NotNull final QueryTable resultTable) {}

    /**
     * Called after initialization; when the operator's result columns must have previous tracking enabled.
     */
    void startTrackingPrevValues();

    /**
     * Initialize refreshing result support for this operator. As a side effect, make a factory method for converting
     * upstream modified column sets to result modified column sets, to be invoked whenever this operator reports a
     * modification in order to determine the operator's contribution to the final result modified column set.
     *
     * @param resultTable The result {@link QueryTable} after initialization
     * @param aggregationUpdateListener The aggregation update listener, which may be needed for referential integrity
     * @return A factory that produces a result modified column set from the upstream modified column set
     */
    default UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        final ModifiedColumnSet resultModifiedColumnSet = resultTable
                .newModifiedColumnSet(getResultColumns().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        return upstreamModifiedColumnSet -> resultModifiedColumnSet;
    }

    /**
     * Reset any per-step internal state. Note that the arguments to this method should not be mutated in any way.
     *
     * @param upstream The upstream ShiftAwareListener.Update
     */
    default void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {}

    /**
     * Perform any internal state keeping needed for destinations that were added (went from 0 keys to &gt 0), removed
     * (went from &gt 0 keys to 0), or modified (keys added or removed, or keys modified) by this iteration. Note that
     * the arguments to this method should not be mutated in any way.
     *
     * @param downstream The downstream ShiftAwareListener.Update (which does <em>not</em> have its
     *        {@link ModifiedColumnSet} finalized yet)
     * @param newDestinations New destinations added on this update
     */
    default void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
            @NotNull final ReadOnlyIndex newDestinations) {}

    /**
     * Called on error to propagate listener failure to this operator.
     *
     * @param originalException The error {@link Throwable}
     * @param sourceEntry The UpdatePerformanceTracker.Entry for the failed listener
     */
    default void propagateFailure(@NotNull final Throwable originalException,
            @NotNull final UpdatePerformanceTracker.Entry sourceEntry) {}

    /**
     * Make a {@link BucketedContext} suitable for this operator if necessary.
     *
     * @param size The maximum size of input chunks that will be used with the result context
     * @return A new {@link BucketedContext}, or null if none is necessary
     */
    default BucketedContext makeBucketedContext(int size) {
        return null;
    }

    /**
     * Make a {@link SingletonContext} suitable for this operator if necessary.
     *
     * @param size The maximum size of input chunks that will be used with the result context
     * @return A new {@link SingletonContext}, or null if none is necessary
     */
    default SingletonContext makeSingletonContext(int size) {
        return null;
    }

    /**
     * Context interface for bucketed operator updates.
     */
    interface BucketedContext extends SafeCloseable {
    }

    /**
     * Context interface for singleton (that is, one aggregation state) operator updates.
     */
    interface SingletonContext extends SafeCloseable {
    }
}
