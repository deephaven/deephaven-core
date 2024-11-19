//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;

abstract class UpdateByWindow {
    @Nullable
    protected final String timestampColumnName;

    /** The operators for this window */
    protected final UpdateByOperator[] operators;

    /** The indices in the UpdateBy input source collection for each operator input slots */
    protected final int[][] operatorInputSourceSlots;

    /** This context will store the necessary info to process a single window for a single bucket */
    static class UpdateByWindowBucketContext implements SafeCloseable {
        /** A reference to the source rowset */
        protected final TrackingRowSet sourceRowSet;
        /** The column source providing the timestamp data for this window */
        @Nullable
        protected final ColumnSource<?> timestampColumnSource;
        /** The timestamp SSA providing fast lookup for time windows */
        @Nullable
        protected final LongSegmentedSortedArray timestampSsa;
        /** This rowset will store row keys where the timestamp is not null (will mirror the SSA contents) */
        protected final TrackingRowSet timestampValidRowSet;
        /** Were any timestamps modified in the current update? */
        protected final boolean timestampsModified;
        /** Whether this is the creation phase of this window */
        protected final boolean initialStep;
        /** Store the key values for this bucket */
        protected final Object[] bucketKeyValues;

        /** An array of ColumnSources for each underlying operator */
        protected ColumnSource<?>[] inputSources;
        /** The rows affected by this update */
        protected RowSet affectedRows;
        /** The rows that will be needed to re-compute `affectedRows` */
        protected RowSet influencerRows;
        /** Size to use for chunked operations */
        protected int workingChunkSize;
        /** Indicates this bucket window needs to be processed */
        protected boolean isDirty;
        /** Indicate which operators need to be processed */
        protected BitSet dirtyOperators;
        protected int[] dirtyOperatorIndices;
        /** Were any input columns modified in the current update? */
        protected boolean inputModified;

        UpdateByWindowBucketContext(final TrackingRowSet sourceRowSet,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa,
                final TrackingRowSet timestampValidRowSet,
                final boolean timestampsModified,
                final int chunkSize,
                final boolean initialStep,
                @NotNull final Object[] bucketKeyValues) {
            this.sourceRowSet = sourceRowSet;
            this.timestampColumnSource = timestampColumnSource;
            this.timestampSsa = timestampSsa;
            this.timestampValidRowSet = timestampValidRowSet;
            this.timestampsModified = timestampsModified;
            this.bucketKeyValues = bucketKeyValues;

            workingChunkSize = chunkSize;
            this.initialStep = initialStep;
        }

        @Override
        public void close() {
            try (final SafeCloseable ignoredRs1 = affectedRows == sourceRowSet ? null : affectedRows) {
                affectedRows = null;
            }
            Assert.eqNull(influencerRows, "influencerRows");
        }
    }

    abstract UpdateByWindowBucketContext makeWindowContext(
            final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final boolean timestampsModified,
            final int chunkSize,
            final boolean isInitializeStep,
            final Object[] bucketKeyValues);

    UpdateByWindow(final UpdateByOperator[] operators,
            final int[][] operatorInputSourceSlots,
            @Nullable final String timestampColumnName) {
        this.operators = operators;
        this.operatorInputSourceSlots = operatorInputSourceSlots;
        this.timestampColumnName = timestampColumnName;
    }

    /**
     * Create a new window and populate it with the specified operators and input source mapping.
     *
     * @param operators the array of operators that belong to this window
     * @param operatorSourceSlots the mapping of operator indices to required input sources indices
     *
     * @return a new {@link UpdateByWindow window} from these operators
     */
    static UpdateByWindow createFromOperatorArray(final UpdateByOperator[] operators,
            final int[][] operatorSourceSlots) {
        // review operators to extract timestamp column (if one exists)
        String timestampColumnName = null;
        for (UpdateByOperator operator : operators) {
            if (operator.getTimestampColumnName() != null) {
                timestampColumnName = operator.getTimestampColumnName();
                break;
            }
        }

        // return the correct type of UpdateByWindow
        if (!operators[0].isWindowed) {
            return new UpdateByWindowCumulative(operators,
                    operatorSourceSlots,
                    timestampColumnName);
        } else if (timestampColumnName == null) {
            return new UpdateByWindowRollingTicks(operators,
                    operatorSourceSlots,
                    operators[0].getPrevWindowUnits(),
                    operators[0].getFwdWindowUnits());
        } else {
            return new UpdateByWindowRollingTime(operators,
                    operatorSourceSlots,
                    timestampColumnName,
                    operators[0].getPrevWindowUnits(),
                    operators[0].getFwdWindowUnits());
        }
    }


    /**
     * Create a new window by copying .
     *
     * @return a new {@link UpdateByWindow window} copied from this one
     */
    abstract UpdateByWindow copy();

    abstract void prepareWindowBucket(UpdateByWindowBucketContext context);

    @OverridingMethodsMustInvokeSuper
    void finalizeWindowBucket(UpdateByWindowBucketContext context) {
        // For efficiency, we occasionally use the source rowsets. Must be careful not to close in these cases.
        // Note that we are not closing affectedRows until the downstream update has been created.
        try (final SafeCloseable ignoredRs2 =
                context.influencerRows == context.affectedRows || context.influencerRows == context.timestampValidRowSet
                        ? null
                        : context.influencerRows) {
            context.influencerRows = null;
        }
    }

    /**
     * Returns the operators for this window (a subset of the total operators for this UpdateBy call)
     */
    UpdateByOperator[] getOperators() {
        return operators;
    }

    /**
     * Returns `true` if the input source is used by this window's operators
     *
     * @param srcIdx the index of the input source
     */
    boolean operatorUsesSource(int winOpIdx, int srcIdx) {
        // this looks worse than it actually is, most operators have exactly one input source and iterating will be
        // faster than building a lookup table or a hashset
        for (int winSrcIdx : operatorInputSourceSlots[winOpIdx]) {
            if (winSrcIdx == srcIdx) {
                return true;
            }
        }
        return false;
    }

    /**
     * Pre-create all the modified/new rows in the output source for parallel update
     *
     * @param changes the rowset indicating which rows will be modified or added this cycle
     */
    void prepareForParallelPopulation(final RowSet changes) {
        for (UpdateByOperator operator : operators) {
            operator.prepareForParallelPopulation(changes);
        }
    }

    // region context-based functions

    /**
     * Examine the upstream {@link TableUpdate update} and determine which operators and rows are affected and will need
     * to be recomputed. This also sets the {@code isDirty} flags on the window context and operator contexts
     *
     * @param context the window context that will store the results.
     * @param upstream the update that indicates incoming changes to the data.
     */
    abstract void computeAffectedRowsAndOperators(final UpdateByWindowBucketContext context,
            @NotNull final TableUpdate upstream);

    /**
     * Accepts and stores the input source array that will be used for future computation. This call allows use of
     * cached input sources instead of potentially slow access to the original input sources
     *
     * @param context the window context that will store these sources.
     * @param inputSources the (potentially cached) input sources to use for processing.
     */
    void assignInputSources(final UpdateByWindowBucketContext context, final ColumnSource<?>[] inputSources) {
        context.inputSources = inputSources;
    }

    /**
     * Perform the computations and store the results in the operator output sources
     *
     * @param context the window context that will manage the results.
     * @param opIndices an array containing indices of the operator within this window to process.
     * @param srcIndices an array containing indices of the operator input sources needed for processing.
     * @param winOpContexts the contexts of the operators to process.
     * @param chunkArr an array of chunks to pass to the operators
     * @param chunkContexts get contexts from the input sources for the operators
     * @param initialStep whether this is the creation step of this bucket.
     */
    abstract void processWindowBucketOperatorSet(UpdateByWindowBucketContext context, int[] opIndices, int[] srcIndices,
            UpdateByOperator.Context[] winOpContexts, Chunk<? extends Values>[] chunkArr,
            ChunkSource.GetContext[] chunkContexts, boolean initialStep);

    /**
     * Returns `true` if the window for this bucket needs to be processed this cycle.
     *
     * @param context the window context that will manage the results.
     */
    boolean isWindowBucketDirty(final UpdateByWindowBucketContext context) {
        return context.isDirty;
    }

    /**
     * Returns the list of `dirty` operators that need to be processed this cycle.
     *
     * @param context the window context that will manage the results.
     */
    int[] getDirtyOperators(final UpdateByWindowBucketContext context) {
        return context.dirtyOperatorIndices;
    }

    /**
     * Returns the rows that will be recomputed for this bucket this cycle
     *
     * @param context the window context that will manage the results.
     */
    RowSet getAffectedRows(final UpdateByWindowBucketContext context) {
        return context.affectedRows;
    }

    /**
     * Returns the rows that are needed to recompute the `affected` rows (that `influence` the results)
     *
     * @param context the window context that will manage the results.
     */
    RowSet getInfluencerRows(final UpdateByWindowBucketContext context) {
        return context.influencerRows;
    }

    /**
     * Examines the {@link TableUpdate update} and set the context dirty bits appropriately
     *
     * @param context the window context that will manage the results.
     * @param upstream the update to process.
     */
    void processUpdateForContext(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {
        boolean addsOrRemoves = upstream.added().isNonempty() || upstream.removed().isNonempty();
        if (addsOrRemoves) {
            // mark all operators as affected by this update
            context.dirtyOperators = new BitSet(operators.length);
            context.dirtyOperators.set(0, operators.length);
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.isDirty = true;
            // still need to compute whether any input columns were modified
            if (upstream.modifiedColumnSet().empty()) {
                return;
            }
            for (UpdateByOperator op : operators) {
                final boolean opInputModified = op.getInputModifiedColumnSet() == null ||
                        upstream.modifiedColumnSet().containsAny(op.getInputModifiedColumnSet());
                if (opInputModified) {
                    context.inputModified = true;
                    break;
                }
            }
        } else if (upstream.modifiedColumnSet().nonempty()) {
            // determine which operators are affected by this update and whether any input columns were modified
            context.dirtyOperators = new BitSet();
            BitSet dirtySourceIndices = new BitSet();
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                UpdateByOperator op = operators[opIdx];

                if (op.getInputModifiedColumnSet() == null ||
                        upstream.modifiedColumnSet().containsAny(op.getInputModifiedColumnSet())) {
                    context.dirtyOperators.set(opIdx);
                    Arrays.stream(operatorInputSourceSlots[opIdx]).forEach(dirtySourceIndices::set);
                    context.inputModified = true;
                }
            }
            context.isDirty = !context.dirtyOperators.isEmpty();
            context.dirtyOperatorIndices = context.dirtyOperators.stream().toArray();
        }
    }

    // endregion
}
