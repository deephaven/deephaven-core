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

        protected final boolean initialStep;

        /** An array of ColumnSources for each underlying operator */
        protected ColumnSource<?>[] inputSources;

        /** An array of {@link ChunkSource.GetContext}s for each input column */
        protected ChunkSource.GetContext[] inputSourceGetContexts;

        /** A set of chunks used to store working values */
        protected Chunk<? extends Values>[] inputSourceChunks;

        /** An indicator of if each slot has been populated with data or not for this phase. */
        protected boolean[] inputSourceChunkPopulated;

        /** the rows affected by this update */
        protected RowSet affectedRows;
        /** the rows that contain values used to compute affected row values */
        protected RowSet influencerRows;

        protected int workingChunkSize;

        /** Indicates this bucket window needs to be processed */
        protected boolean isDirty;

        /** Indicates which operators need to be processed */
        protected int[] dirtyOperatorIndices;

        /** Not actually dity, but indicates which sources are need to process this window context */
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

    @Nullable
    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    public UpdateByOperator[] getOperators() {
        return operators;
    }

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

    // region context-based functions

    public abstract void computeAffectedRowsAndOperators(final UpdateByWindowContext context,
            @NotNull final TableUpdate upstream);

    protected abstract void makeOperatorContexts(final UpdateByWindowContext context);

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

    public abstract void processRows(final UpdateByWindowContext context, final boolean initialStep);

    public boolean isWindowDirty(final UpdateByWindowContext context) {
        return context.isDirty;
    }

    public int[] getDirtyOperators(final UpdateByWindowContext context) {
        return context.dirtyOperatorIndices;
    }

    public int[] getDirtySources(final UpdateByWindowContext context) {
        return context.dirtySourceIndices;
    }

    public RowSet getAffectedRows(final UpdateByWindowContext context) {
        return context.affectedRows;
    }

    public RowSet getInfluencerRows(final UpdateByWindowContext context) {
        return context.influencerRows;
    }

    // endregion

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

    public static int hashCodeFromOperator(final UpdateByOperator op) {
        return hashCode(op instanceof UpdateByWindowedOperator,
                op.getInputColumnNames(),
                op.getTimestampColumnName(),
                op.getPrevWindowUnits(),
                op.getPrevWindowUnits());
    }

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
