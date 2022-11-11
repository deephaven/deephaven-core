package io.deephaven.engine.table.impl.updateby;

import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class UpdateByWindow {
    @Nullable
    protected final String timestampColumnName;

    // store the operators for this window
    protected final UpdateByOperator[] operators;

    // store the index in the {@link UpdateBy.inputSources}
    protected final int[][] operatorInputSourceSlots;

    protected int[] uniqueInputSourceIndices;

    /** This context will store the necessary info to process a single window for a single bucket */
    public abstract class UpdateByWindowContext implements SafeCloseable {
        /** store a reference to the source rowset */
        protected final TrackingRowSet sourceRowSet;
        /** the column source providing the timestamp data for this window */
        @Nullable
        protected final ColumnSource<?> timestampColumnSource;
        /** the timestamp SSA providing fast lookup for time windows */
        @Nullable
        protected final LongSegmentedSortedArray timestampSsa;
        /** An array of context objects for each underlying operator */
        protected final UpdateByOperator.UpdateContext[] opContext;
        /** Whether this is the creation phase of this operator */
        protected final boolean initialStep;

        /** An array of ColumnSources for each underlying operator */
        protected ColumnSource<?>[] inputSources;
        /** An array of {@link ChunkSource.GetContext}s for each input column */
        protected ChunkSource.GetContext[] inputSourceGetContexts;
        /** A set of chunks used to store working values */
        protected Chunk<? extends Values>[] inputSourceChunks;
        /** An indicator of if each slot has been populated with data or not for this phase. */
        protected boolean[] inputSourceChunkPopulated;
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

        public UpdateByWindowContext(final TrackingRowSet sourceRowSet,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            this.sourceRowSet = sourceRowSet;
            this.timestampColumnSource = timestampColumnSource;
            this.timestampSsa = timestampSsa;

            this.opContext = new UpdateByOperator.UpdateContext[operators.length];

            this.workingChunkSize = chunkSize;
            this.initialStep = initialStep;
            this.isDirty = false;
        }

        @Override
        public void close() {
            // these might be the same object, don't close both!
            if (influencerRows != null && influencerRows != affectedRows) {
                influencerRows.close();
                influencerRows = null;
            }
            try (final RowSet ignoredRs1 = affectedRows) {
            }
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opContext[opIdx] != null) {
                    opContext[opIdx].close();
                }
            }
            if (inputSources != null) {
                for (int srcIdx = 0; srcIdx < inputSources.length; srcIdx++) {
                    if (inputSourceGetContexts[srcIdx] != null) {
                        inputSourceGetContexts[srcIdx].close();
                        inputSourceGetContexts[srcIdx] = null;
                    }
                }
            }
        }
    }

    public abstract UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep);

    protected UpdateByWindow(UpdateByOperator[] operators, int[][] operatorInputSourceSlots,
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
    public static UpdateByWindow createFromOperatorArray(final UpdateByOperator[] operators,
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
        final boolean windowed = operators[0] instanceof UpdateByWindowedOperator;
        if (!windowed) {
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

    /**
     * Returns the timestamp column name for this window (or null if no timestamps in use)
     */
    @Nullable
    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    /**
     * Returns the operators for this window (a subset of the total operators for this UpdateBy call)
     */
    public UpdateByOperator[] getOperators() {
        return operators;
    }

    /**
     * Returns the mapping from operator indices to input source indices
     */
    public int[][] getOperatorInputSourceSlots() {
        return operatorInputSourceSlots;
    }

    public int[] getUniqueSourceIndices() {
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
    public boolean isSourceInUse(int srcIdx) {
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
     * Pre-create all the modified/new rows in the output source so they can be updated in parallel tasks
     *
     * @param changes the rowset indicating which rows will be modifed or added this cycle
     */
    public void prepareForParallelPopulation(final RowSet changes) {
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
    public abstract void computeAffectedRowsAndOperators(final UpdateByWindowContext context,
            @NotNull final TableUpdate upstream);

    /**
     * Generate the contexts used by the operators for this bucket.
     *
     * @param context the window context that will store the results.
     */
    protected abstract void makeOperatorContexts(final UpdateByWindowContext context);

    /**
     * Accepts and stores the input source array that will be used for future computation. This call allows use of
     * cached input sources instead of potentially slow access to the original input sources
     *
     * @param context the window context that will store these sources.
     * @param inputSources the (potentially cached) input sources to use for processing.
     */
    public void assignInputSources(final UpdateByWindowContext context, final ColumnSource<?>[] inputSources) {
        context.inputSources = inputSources;
        context.inputSourceChunkPopulated = new boolean[inputSources.length];
        context.inputSourceGetContexts = new ChunkSource.GetContext[inputSources.length];
        // noinspection unchecked
        context.inputSourceChunks = new WritableChunk[inputSources.length];

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
    protected void prepareValuesChunkForSource(final UpdateByWindowContext context, final int srcIdx,
            final RowSequence rs) {
        if (rs.isEmpty()) {
            return;
        }
        if (!context.inputSourceChunkPopulated[srcIdx]) {
            context.inputSourceChunks[srcIdx] =
                    context.inputSources[srcIdx].getChunk(context.inputSourceGetContexts[srcIdx], rs);
            context.inputSourceChunkPopulated[srcIdx] = true;
        }
    }

    /**
     * Perform the computations and store the results in the operator output sources
     *
     * @param context the window context that will manage the results.
     * @param initialStep whether this is the creation step of the table.
     */
    public abstract void processRows(final UpdateByWindowContext context, final boolean initialStep);

    /**
     * Returns `true` if the window for this bucket needs to be processed this cycle.
     *
     * @param context the window context that will manage the results.
     */
    public boolean isWindowDirty(final UpdateByWindowContext context) {
        return context.isDirty;
    }

    /**
     * Returns `true` if the window for this bucket needs to be processed this cycle.
     *
     * @param context the window context that will manage the results.
     */
    public int[] getDirtyOperators(final UpdateByWindowContext context) {
        return context.dirtyOperatorIndices;
    }

    /**
     * Returns the list of input sources that will be needed to process the `dirty` operators for this bucket
     *
     * @param context the window context that will manage the results.
     */
    public int[] getDirtySources(final UpdateByWindowContext context) {
        return context.dirtySourceIndices;
    }

    /**
     * Returns the rows that will be recomputed for this bucket this cycle
     *
     * @param context the window context that will manage the results.
     */
    public RowSet getAffectedRows(final UpdateByWindowContext context) {
        return context.affectedRows;
    }

    /**
     * Returns the rows that are needed to recompute the `affected` rows (that `influence` the results)
     *
     * @param context the window context that will manage the results.
     */
    public RowSet getInfluencerRows(final UpdateByWindowContext context) {
        return context.influencerRows;
    }

    // endregion

    /**
     * Returns a hash code to help distinguish between windows on the same UpdateBy call
     */
    protected static int hashCode(boolean windowed, @NotNull String[] inputColumnNames,
            @Nullable String timestampColumnName, long prevUnits,
            long fwdUnits) {

        // hash the input column names
        int hash = 0;
        for (String s : inputColumnNames) {
            hash = 31 * hash + s.hashCode();
        }

        // treat all cumulative ops with the same input columns as identical, even if they rely on timestamps
        if (!windowed) {
            return 31 * hash + Boolean.hashCode(false);
        }

        // windowed ops are unique per type (ticks/time-based) and window dimensions
        hash = 31 * hash + Boolean.hashCode(true);
        hash = 31 * hash + Boolean.hashCode(timestampColumnName != null);
        hash = 31 * hash + Long.hashCode(prevUnits);
        hash = 31 * hash + Long.hashCode(fwdUnits);
        return hash;
    }

    /**
     * Returns a hash code given a particular operator
     */
    public static int hashCodeFromOperator(final UpdateByOperator op) {
        return hashCode(op instanceof UpdateByWindowedOperator,
                op.getInputColumnNames(),
                op.getTimestampColumnName(),
                op.getPrevWindowUnits(),
                op.getPrevWindowUnits());
    }

    /**
     * Returns `true` if two operators are compatible and can be executed as part of the same window
     */
    public static boolean isEquivalentWindow(final UpdateByOperator opA, final UpdateByOperator opB) {
        // verify input columns match
        String[] opAInput = opA.getInputColumnNames();
        String[] opBInput = opB.getInputColumnNames();

        if (opAInput.length != opBInput.length) {
            return false;
        }
        for (int ii = 0; ii < opAInput.length; ii++) {
            if (!opAInput[ii].equals(opBInput[ii])) {
                return false;
            }
        }

        final boolean aWindowed = opA instanceof UpdateByWindowedOperator;
        final boolean bWindowed = opB instanceof UpdateByWindowedOperator;

        // equivalent if both are cumulative, not equivalent if only one is cumulative
        if (!aWindowed && !bWindowed) {
            return true;
        } else if (!aWindowed) {
            return false;
        } else if (!bWindowed) {
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
