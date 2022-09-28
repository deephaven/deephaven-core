package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class UpdateByWindow {
    @Nullable
    protected final String timestampColumnName;

    // store the operators for this window
    protected final UpdateByOperator[] operators;
    // store the index in the {@link UpdateBy.inputSources}
    protected final int[] operatorSourceSlots;
    // individual input/output modifiedColumnSets for the operators
    protected final ModifiedColumnSet[] operatorInputModifiedColumnSets;
    protected final ModifiedColumnSet[] operatorOutputModifiedColumnSets;

    protected boolean trackModifications;

    public abstract class UpdateByWindowContext implements SafeCloseable {
        /** store a reference to the source rowset */
        protected final TrackingRowSet sourceRowSet;

        /** the column source providing the timestamp data for this window */
        @Nullable
        protected final ColumnSource<?> timestampColumnSource;

        /** the timestamp SSA providing fast lookup for time windows */
        @Nullable
        protected final LongSegmentedSortedArray timestampSsa;

        /** An array of boolean denoting which operators are affected by the current update. */
        protected final boolean[] opAffected;

        /** An array of context objects for each underlying operator */
        protected final UpdateByOperator.UpdateContext[] opContext;

        /** An array of ColumnSources for each underlying operator */
        protected final ColumnSource<?>[] inputSources;

        /** An array of {@link ChunkSource.FillContext}s for each input column */
        protected final ChunkSource.FillContext[] inputSourceFillContexts;

        /** A set of chunks used to store working values */
        protected final WritableChunk<Values>[] inputSourceChunks;

        /** An indicator of if each slot has been populated with data or not for this phase. */
        protected final boolean[] inputSourceChunkPopulated;

        /** the rows affected by this update */
        protected RowSet affectedRows;
        /** the rows that contain values used to compute affected row values */
        protected RowSet influencerRows;

        /** keep track of what rows were modified (we'll use a single set for all operators sharing a window) */
        protected RowSetBuilderSequential modifiedBuilder;
        protected RowSet newModified;

        protected final int chunkSize;
        protected final boolean initialStep;

        public UpdateByWindowContext(final TrackingRowSet sourceRowSet, final ColumnSource<?>[] inputSources,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            this.sourceRowSet = sourceRowSet;
            this.inputSources = inputSources;
            this.timestampColumnSource = timestampColumnSource;
            this.timestampSsa = timestampSsa;

            this.opAffected = new boolean[operators.length];
            this.opContext = new UpdateByOperator.UpdateContext[operators.length];
            this.inputSourceFillContexts = new ChunkSource.FillContext[operators.length];
            this.inputSourceChunkPopulated = new boolean[operators.length];
            // noinspection unchecked
            this.inputSourceChunks = new WritableChunk[operators.length];

            this.chunkSize = chunkSize;
            this.initialStep = initialStep;
        }

        public abstract boolean computeAffectedRowsAndOperators(@NotNull final TableUpdate upstream);

        public abstract void processRows();

        protected void makeOperatorContexts() {
            // use this to make which input sources are initialized
            Arrays.fill(inputSourceChunkPopulated, false);

            // create contexts for the affected operators
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    // create the fill contexts for the input sources
                    int sourceSlot = operatorSourceSlots[opIdx];
                    if (!inputSourceChunkPopulated[sourceSlot]) {
                        inputSourceChunks[sourceSlot] =
                                inputSources[sourceSlot].getChunkType().makeWritableChunk(chunkSize);
                        inputSourceFillContexts[sourceSlot] = inputSources[sourceSlot].makeFillContext(chunkSize);
                        inputSourceChunkPopulated[sourceSlot] = true;
                    }
                    opContext[opIdx] = operators[opIdx].makeUpdateContext(chunkSize, inputSources[sourceSlot]);
                }
            }
        }

        public boolean anyModified() {
            return newModified != null && newModified.isNonempty();
        }

        public RowSet getModifiedRows() {
            return newModified;
        }

        public void updateOutputModifiedColumnSet(ModifiedColumnSet outputModifiedColumnSet) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    outputModifiedColumnSet.setAll(operatorOutputModifiedColumnSets[opIdx]);
                }
            }
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public RowSet getInfluencerRows() {
            return influencerRows;
        }

        protected void prepareValuesChunkForSource(final int srcIdx, final RowSequence rs) {
            if (!inputSourceChunkPopulated[srcIdx]) {
                inputSources[srcIdx].fillChunk(
                        inputSourceFillContexts[srcIdx],
                        inputSourceChunks[srcIdx],
                        rs);
                inputSourceChunkPopulated[srcIdx] = true;
            }
        }

        @Override
        public void close() {
            // these might be the same object, don't close both!
            if (influencerRows != null && influencerRows != affectedRows) {
                influencerRows.close();
                influencerRows = null;
            }
            try (final RowSet ignoredRs1 = affectedRows;
                    final RowSet ignoredRs2 = newModified) {
            }
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    final int srcIdx = operatorSourceSlots[opIdx];
                    if (inputSourceChunks[srcIdx] != null) {

                        inputSourceChunks[srcIdx].close();
                        inputSourceChunks[srcIdx] = null;

                        inputSourceFillContexts[srcIdx].close();
                        inputSourceFillContexts[srcIdx] = null;
                    }
                    opContext[opIdx].close();
                }
            }
        }
    }

    public abstract UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?>[] inputSources,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep);

    public void startTrackingModifications(@NotNull final QueryTable source, @NotNull final QueryTable result) {
        trackModifications = true;
        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
            operatorInputModifiedColumnSets[opIdx] =
                    source.newModifiedColumnSet(operators[opIdx].getAffectingColumnNames());
            operatorOutputModifiedColumnSets[opIdx] =
                    result.newModifiedColumnSet(operators[opIdx].getOutputColumnNames());
        }
    }

    protected UpdateByWindow(UpdateByOperator[] operators, int[] operatorSourceSlots,
            @Nullable String timestampColumnName) {
        this.operators = operators;
        this.operatorSourceSlots = operatorSourceSlots;
        this.timestampColumnName = timestampColumnName;

        operatorInputModifiedColumnSets = new ModifiedColumnSet[operators.length];
        operatorOutputModifiedColumnSets = new ModifiedColumnSet[operators.length];
        trackModifications = false;
    }

    public static UpdateByWindow createFromOperatorArray(final UpdateByOperator[] operators,
            final int[] operatorSourceSlots) {
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

    protected static int hashCode(boolean windowed, @Nullable String timestampColumnName, long prevUnits,
            long fwdUnits) {
        // treat all cumulative as identical, even if they rely on timestamps
        if (!windowed) {
            return Boolean.hashCode(windowed);
        }

        // windowed are unique per timestamp column and window-size
        int hash = Boolean.hashCode(windowed);
        if (timestampColumnName != null) {
            hash = 31 * hash + timestampColumnName.hashCode();
        }
        hash = 31 * hash + Long.hashCode(prevUnits);
        hash = 31 * hash + Long.hashCode(fwdUnits);
        return hash;
    }

    public static int hashCodeFromOperator(final UpdateByOperator op) {
        return hashCode(op instanceof UpdateByWindowedOperator,
                op.getTimestampColumnName(),
                op.getPrevWindowUnits(),
                op.getPrevWindowUnits());
    }
}
