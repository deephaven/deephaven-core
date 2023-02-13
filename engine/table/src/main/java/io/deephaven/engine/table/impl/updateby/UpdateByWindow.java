package io.deephaven.engine.table.impl.updateby;

import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.stream.IntStream;

abstract class UpdateByWindow {
    @Nullable
    protected final String timestampColumnName;

    /** The operators for this window */
    protected final UpdateByOperator[] operators;

    /** The indices in the UpdateBy input source collection for each operator input slots */
    protected final int[][] operatorInputSourceSlots;

    protected int[] uniqueInputSourceIndices;

    /** This context will store the necessary info to process a single window for a single bucket */
    class UpdateByWindowBucketContext implements SafeCloseable {
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

        /** An array of context objects for each underlying operator */
        protected UpdateByOperator.Context[] opContexts;
        /** An array of ColumnSources for each underlying operator */
        protected ColumnSource<?>[] inputSources;
        /** An array of {@link ChunkSource.GetContext}s for each input column */
        protected ChunkSource.GetContext[] inputSourceGetContexts;
        /** A set of chunks used to store working values */
        protected Chunk<? extends Values>[] inputSourceChunks;
        /** The rows affected by this update */
        protected RowSet affectedRows;
        /** The rows that will be needed to re-compute `affectedRows` */
        protected RowSet influencerRows;
        /** Size to use for chunked operations */
        protected int workingChunkSize;
        /** Indicates this bucket window needs to be processed */
        protected boolean isDirty;
        /** Indicates which operators need to be processed */
        protected int[] dirtyOperatorIndices;
        /** Indicates which sources are needed to process this window context */
        protected int[] dirtySourceIndices;
        /** Were any input columns modified in the current update? */
        protected boolean inputModified;

        UpdateByWindowBucketContext(final TrackingRowSet sourceRowSet,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa,
                final TrackingRowSet timestampValidRowSet,
                final boolean timestampsModified,
                final int chunkSize,
                final boolean initialStep) {
            this.sourceRowSet = sourceRowSet;
            this.timestampColumnSource = timestampColumnSource;
            this.timestampSsa = timestampSsa;
            this.timestampValidRowSet = timestampValidRowSet;
            this.timestampsModified = timestampsModified;

            workingChunkSize = chunkSize;
            this.initialStep = initialStep;
        }

        @Override
        public void close() {
            try (final SafeCloseable ignoredRs1 = affectedRows == sourceRowSet ? null : affectedRows) {
                affectedRows = null;
            }
            Assert.eqNull(influencerRows, "influencerRows");
            Assert.eqNull(opContexts, "opContexts");
            Assert.eqNull(inputSourceGetContexts, "inputSourceGetContexts");
        }
    }

    abstract UpdateByWindowBucketContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final boolean timestampsModified,
            final int chunkSize,
            final boolean isInitializeStep);

    UpdateByWindow(UpdateByOperator[] operators, int[][] operatorInputSourceSlots,
            @Nullable String timestampColumnName) {
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
            return new UpdateByWindowTicks(operators,
                    operatorSourceSlots,
                    operators[0].getPrevWindowUnits(),
                    operators[0].getFwdWindowUnits());
        } else {
            return new UpdateByWindowTime(operators,
                    operatorSourceSlots,
                    timestampColumnName,
                    operators[0].getPrevWindowUnits(),
                    operators[0].getFwdWindowUnits());
        }
    }

    @OverridingMethodsMustInvokeSuper
    void allocateResources(UpdateByWindowBucketContext context) {
        context.opContexts = new UpdateByOperator.Context[operators.length];
    }
    @OverridingMethodsMustInvokeSuper
    void releaseResources(UpdateByWindowBucketContext context) {
        // For efficiency, we occasionally use the source rowsets. Must be careful not to close in these cases.
        // Note that we are not closing affectedRows until the downstream update has been created.
        try (final SafeCloseable ignoredRs2 =
                     context.influencerRows == context.affectedRows || context.influencerRows == context.timestampValidRowSet ? null
                             : context.influencerRows) {
            context.influencerRows = null;
        }
        if (context.opContexts != null) {
            SafeCloseableArray.close(context.opContexts);
            context.opContexts = null;
        }
        if (context.inputSourceGetContexts != null) {
            SafeCloseableArray.close(context.inputSourceGetContexts);
            context.inputSourceGetContexts = null;
        }
    }

    /**
     * Returns the operators for this window (a subset of the total operators for this UpdateBy call)
     */
    UpdateByOperator[] getOperators() {
        return operators;
    }

    int[] getUniqueSourceIndices() {
        if (uniqueInputSourceIndices == null) {
            final TIntHashSet set = new TIntHashSet();
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                set.addAll(operatorInputSourceSlots[opIdx]);
            }
            uniqueInputSourceIndices = set.toArray();
        }
        return uniqueInputSourceIndices;
    }

    /**
     * Returns `true` if the input source is used by this window's operators
     *
     * @param srcIdx the index of the input source
     */
    boolean isSourceInUse(int srcIdx) {
        // this looks worse than it actually is, windows are defined by their input sources so there will be only
        // one or two entries in `getUniqueSourceIndices()`. Iterating will be faster than building a lookup table
        // or a hashset
        for (int winSrcIdx : getUniqueSourceIndices()) {
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
        context.inputSourceGetContexts = new ChunkSource.GetContext[inputSources.length];
        context.inputSourceChunks = new Chunk[inputSources.length];

        for (int srcIdx : context.dirtySourceIndices) {
            context.inputSourceGetContexts[srcIdx] =
                    context.inputSources[srcIdx].makeGetContext(context.workingChunkSize);
        }
    }

    /**
     * Prepare a chunk of data from this input source for later computations
     *
     * @param context the window context that will store the results.
     * @param srcIdx the index of the input source.
     * @param rs the rows to retrieve.
     */
    protected void prepareValuesChunkForSource(final UpdateByWindowBucketContext context, final int srcIdx,
            final RowSequence rs) {
        if (context.inputSourceChunks[srcIdx] == null) {
            context.inputSourceChunks[srcIdx] =
                    context.inputSources[srcIdx].getChunk(context.inputSourceGetContexts[srcIdx], rs);
        }
    }

    /**
     * Perform the computations and store the results in the operator output sources
     *
     * @param context the window context that will manage the results.
     * @param initialStep whether this is the creation step of the table.
     */
    abstract void processRows(final UpdateByWindowBucketContext context, final boolean initialStep);

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
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();
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
            BitSet dirtyOperators = new BitSet();
            BitSet dirtySourceIndices = new BitSet();
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                UpdateByOperator op = operators[opIdx];
                final boolean opInputModified = op.getInputModifiedColumnSet() == null ||
                        upstream.modifiedColumnSet().containsAny(op.getInputModifiedColumnSet());
                if (opInputModified) {
                    dirtyOperators.set(opIdx);
                    Arrays.stream(operatorInputSourceSlots[opIdx]).forEach(dirtySourceIndices::set);
                    context.inputModified = true;
                }
            }
            context.isDirty = !dirtyOperators.isEmpty();
            context.dirtyOperatorIndices = dirtyOperators.stream().toArray();
            context.dirtySourceIndices = dirtySourceIndices.stream().toArray();
        }
    }

    // endregion

    /**
     * Returns a hash code to help distinguish between windows on the same UpdateBy call
     */
    private static int hashCode(boolean windowed, @NotNull String[] inputColumnNames,
            @Nullable String timestampColumnName, long prevUnits,
            long fwdUnits) {

        // hash the input column names
        int hash = 0;
        for (String s : inputColumnNames) {
            hash = 31 * hash + s.hashCode();
        }

        hash = 31 * hash + Boolean.hashCode(windowed);

        // treat all cumulative ops with the same input columns as identical, even if they rely on timestamps
        if (!windowed) {
            return hash;
        }

        // windowed ops are unique per type (ticks/time-based) and window dimensions
        hash = 31 * hash + Objects.hashCode(timestampColumnName);
        hash = 31 * hash + Long.hashCode(prevUnits);
        hash = 31 * hash + Long.hashCode(fwdUnits);
        return hash;
    }

    /**
     * Returns a hash code given a particular operator
     */
    static int hashCodeFromOperator(final UpdateByOperator op) {
        return hashCode(op.isWindowed,
                op.getInputColumnNames(),
                op.getTimestampColumnName(),
                op.getPrevWindowUnits(),
                op.getFwdWindowUnits());
    }

    /**
     * Returns `true` if two operators are compatible and can be executed as part of the same window
     */
    static boolean isEquivalentWindow(final UpdateByOperator opA, final UpdateByOperator opB) {
        // verify input columns are identical
        if (!Arrays.equals(opA.getInputColumnNames(), opB.getInputColumnNames())) {
            return false;
        }

        // equivalent if both are cumulative, not equivalent if only one is cumulative
        if (!opA.isWindowed && !opB.isWindowed) {
            return true;
        } else if (opA.isWindowed != opB.isWindowed) {
            return false;
        }

        final boolean aTimeWindowed = opA.getTimestampColumnName() != null;
        final boolean bTimeWindowed = opB.getTimestampColumnName() != null;

        // must have same time/tick base to be equivalent
        if (aTimeWindowed != bTimeWindowed) {
            return false;
        }
        return opA.getPrevWindowUnits() == opB.getPrevWindowUnits() &&
                opB.getFwdWindowUnits() == opB.getFwdWindowUnits();
    }
}
