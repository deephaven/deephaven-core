package io.deephaven.engine.table.impl.updateby.rollinggroup;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.aggregate.*;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class RollingGroupOperator extends UpdateByOperator {
    /**
     * Store input/output column information for retrieval.
     */
    private final String[] inputColumnNames;
    private final String[] outputColumnNames;
    private final ColumnSource<?>[] outputSources;
    private final Map<String, ColumnSource<?>> outputSourceMap;


    /** Store a mapping from row keys to bucket RowSets */
    protected final WritableColumnSource<? extends RowSet> groupRowSetSource;
    protected final ObjectArraySource<? extends RowSet> innerGroupRowSetSource;

    /**
     * These sources will retain the position offsets from the current row position for the source and end of this
     * window. A primary benefit of storing offsets in position space vs. row keys is eliminating the need to shift keys
     * stored inside these column sources. Also, any insertion/modification/remove that occurs within this specified
     * window will trigger re-computation of these position offsets.
     * <p>
     * NOTE: these offsets are inclusive and are stored as simple relative positions where start==0 or end==0 implies
     * the current row is included in the window. A start/end range of [-5,-3] defines a window including exactly 3 rows
     * beginning 5 rows earlier than this one and continuing through 3 rows earlier than this (inclusive). A range of
     * [0,0] contains exactly the current row.
     */
    protected final WritableColumnSource<Long> startSource;
    protected final LongArraySource innerStartSource;
    protected final WritableColumnSource<Long> endSource;
    protected final LongArraySource innerEndSource;

    private ModifiedColumnSet.Transformer inputOutputTransformer;
    private ModifiedColumnSet[] outputModifiedColumnSets;

    public class Context extends UpdateByOperator.Context {
        private static final int BUFFER_INITIAL_CAPACITY = 512;
        public final ChunkSink.FillFromContext groupRowSetSourceFillFromContext;
        public final WritableObjectChunk<RowSet, ? extends Values> groupRowSetSourceOutputValues;
        public final ChunkSink.FillFromContext startSourceFillFromContext;
        public final WritableLongChunk<Values> startSourceOutputValues;
        public final ChunkSink.FillFromContext endSourceFillFromContext;
        public final WritableLongChunk<Values> endSourceOutputValues;
        private final LongRingBuffer windowKeys;
        private long startPos = NULL_LONG;
        private long endPos = NULL_LONG;

        protected Context(final int chunkSize) {
            groupRowSetSourceFillFromContext = groupRowSetSource.makeFillFromContext(chunkSize);
            groupRowSetSourceOutputValues = WritableObjectChunk.makeWritableChunk(chunkSize);
            if (timestampColumnName != null) {
                startSourceFillFromContext = startSource.makeFillFromContext(chunkSize);
                startSourceOutputValues = WritableLongChunk.makeWritableChunk(chunkSize);
                endSourceFillFromContext = endSource.makeFillFromContext(chunkSize);
                endSourceOutputValues = WritableLongChunk.makeWritableChunk(chunkSize);
                windowKeys = new LongRingBuffer(BUFFER_INITIAL_CAPACITY, true);
            } else {
                startSourceFillFromContext = null;
                startSourceOutputValues = null;
                endSourceFillFromContext = null;
                endSourceOutputValues = null;
                windowKeys = null;
            }
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(
                    groupRowSetSourceFillFromContext,
                    groupRowSetSourceOutputValues,
                    startSourceFillFromContext,
                    startSourceOutputValues,
                    endSourceFillFromContext,
                    endSourceOutputValues);
        }

        @Override
        public void accumulateCumulative(RowSequence inputKeys, Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk, int len) {
            throw new IllegalStateException("accumulateCumulative() is invalid for RollingGroupOperator");
        }

        @Override
        public void accumulateRolling(RowSequence inputKeys,
                Chunk<? extends Values>[] influencerValueChunkArr,
                LongChunk<OrderedRowKeys> affectedPosChunk,
                LongChunk<OrderedRowKeys> influencerPosChunk,
                IntChunk<? extends Values> pushChunk,
                IntChunk<? extends Values> popChunk,
                int len) {

            if (timestampColumnName == null) {
                // The only work for ticks operators is to update the groupRowSetSource
                groupRowSetSource.fillFromChunk(groupRowSetSourceFillFromContext, groupRowSetSourceOutputValues,
                        inputKeys);
                return;
            }

            setPosChunks(affectedPosChunk, influencerPosChunk);
            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                final int pushCount = pushChunk.get(ii);
                final int popCount = popChunk.get(ii);

                if (pushCount == NULL_INT) {
                    // Setting NULL_LONG, NULL_LONG marks this as an invalid row.
                    startSourceOutputValues.set(ii, NULL_LONG);
                    endSourceOutputValues.set(ii, NULL_LONG);
                    continue;
                }

                // pop for this row
                if (popCount > 0) {
                    pop(popCount);
                }

                // push for this row
                if (pushCount > 0) {
                    push(pushIndex, pushCount);
                    pushIndex += pushCount;
                }

                // write the results to the output chunk
                writeToOutputChunk(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            // NOP
        }

        @Override
        public void push(int pos, int count) {
            windowKeys.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                endPos = influencerPosChunk.get(pos + ii);
                windowKeys.addUnsafe(endPos);
                if (startPos == NULL_LONG) {
                    startPos = endPos;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowKeys.size(), "windowKeys.size()", count);

            for (int ii = 0; ii < count; ii++) {
                windowKeys.removeUnsafe();
                if (!windowKeys.isEmpty()) {
                    startPos = windowKeys.front();
                } else {
                    startPos = NULL_LONG;
                    endPos = NULL_LONG;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (startPos == NULL_LONG) {
                // Setting NULL_LONG, 0 signifies empty window
                startSourceOutputValues.set(outIdx, NULL_LONG);
                endSourceOutputValues.set(outIdx, 0);
            } else {
                final long affectedPos = affectedPosChunk.get(outIdx);
                startSourceOutputValues.set(outIdx, startPos - affectedPos);
                // Store endPos as an exclusive value by incrementing by one
                endSourceOutputValues.set(outIdx, endPos - affectedPos + 1);
            }
        }

        @Override
        public void reset() {
            startPos = NULL_LONG;
            endPos = NULL_LONG;
            if (windowKeys != null) {
                windowKeys.clear();
            }
        }

        @Override
        public void writeToOutputColumn(@NotNull RowSequence inputKeys) {
            startSource.fillFromChunk(startSourceFillFromContext, startSourceOutputValues, inputKeys);
            endSource.fillFromChunk(endSourceFillFromContext, endSourceOutputValues, inputKeys);
            groupRowSetSource.fillFromChunk(groupRowSetSourceFillFromContext, groupRowSetSourceOutputValues, inputKeys);
        }
    }

    public RollingGroupOperator(@NotNull final MatchPair[] pairs,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final ColumnSource<?>[] valueSources
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pairs[0], affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true);

        inputColumnNames = new String[pairs.length];
        outputColumnNames = new String[pairs.length];
        outputSources = new ColumnSource[pairs.length];
        outputSourceMap = new HashMap<>();

        // For the sake of rolling group operators, we need to map from every row to its bucket row set (or the
        // non-null version) and to the start and end ranges of keys for the window slice. If we are using
        // redirection, use it for these structures as well.
        if (rowRedirection != null) {
            innerGroupRowSetSource = new ObjectArraySource<>(RowSet.class);
            groupRowSetSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerGroupRowSetSource, 0);

        } else {
            groupRowSetSource = new ObjectSparseArraySource<>(RowSet.class);
            innerGroupRowSetSource = null;
        }

        // If we are computing through ticks, we won't need sources for start and end.
        if (timestampColumnName == null) {
            innerStartSource = null;
            innerEndSource = null;
            startSource = null;
            endSource = null;
        } else {
            // We are computing using the timestamp columns, create the appropriate sources (potentially redirected)
            if (rowRedirection == null) {
                startSource = new LongSparseArraySource();
                endSource = new LongSparseArraySource();
                innerStartSource = null;
                innerEndSource = null;
            } else {
                innerStartSource = new LongArraySource();
                innerEndSource = new LongArraySource();
                startSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerStartSource, 0);
                endSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerEndSource, 0);
            }
        }

        for (int ii = 0; ii < pairs.length; ii++) {
            final MatchPair pair = pairs[ii];
            final Class<?> csType = valueSources[ii].getType();

            inputColumnNames[ii] = pair.rightColumn;
            outputColumnNames[ii] = pair.leftColumn;

            // When timestampColumnName == null, we have a tick-based rolling window. RollingOpSpec accepts fwd/rev
            // tick parameters and applies the constraint that the current row belongs to the reverse window. This
            // implies that to create a group containing exactly the current row, you must provide a rev/fwd range of
            // [1, 0]. This constraint is useful for the user but should not propagate to the general purpose
            // output AggregateColumnSource constructors. We are converting the Rolling window range of values to a
            // simple +/- relative positional offset range where [0, 0] implies a group that contains only the current
            // row. Similarly, a user range of [5, -3] will convert to [-4, -2) and in both cases will be a group of
            // two rows starting 4 rows before the current row.
            //
            // The aggregated column source range is half-open, so we add one to the inclusive fwd units to convert.
            outputSources[ii] = timestampColumnName != null
                    ? AggregateColumnSource.makeSliced((ColumnSource<Character>) valueSources[ii], groupRowSetSource,
                            startSource, endSource)
                    : AggregateColumnSource.makeSliced((ColumnSource<Character>) valueSources[ii], groupRowSetSource,
                            -reverseWindowScaleUnits + 1, forwardWindowScaleUnits + 1);
            outputSourceMap.put(outputColumnNames[ii], outputSources[ii]);
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int influencerChunkSize) {
        return new Context(chunkSize);
    }

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        if (rowRedirection != null) {
            assert innerGroupRowSetSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) innerGroupRowSetSource)
                    .prepareForParallelPopulation(changedRows);
            if (timestampColumnName != null) {
                assert innerStartSource != null;
                ((WritableSourceWithPrepareForParallelPopulation) innerStartSource)
                        .prepareForParallelPopulation(changedRows);
                assert innerEndSource != null;
                ((WritableSourceWithPrepareForParallelPopulation) innerEndSource)
                        .prepareForParallelPopulation(changedRows);
            }
        } else {
            ((WritableSourceWithPrepareForParallelPopulation) groupRowSetSource)
                    .prepareForParallelPopulation(changedRows);
            if (timestampColumnName != null) {
                ((WritableSourceWithPrepareForParallelPopulation) startSource)
                        .prepareForParallelPopulation(changedRows);
                ((WritableSourceWithPrepareForParallelPopulation) endSource).prepareForParallelPopulation(changedRows);
            }
        }
    }

    @Override
    public void initializeRolling(@NotNull final UpdateByOperator.Context context, @NotNull final RowSet bucketRowSet) {
        super.initializeRolling(context, bucketRowSet);

        Context ctx = (Context) context;
        ctx.groupRowSetSourceOutputValues.fillWithValue(0, ctx.groupRowSetSourceOutputValues.size(), bucketRowSet);
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((ObjectSparseArraySource<?>) groupRowSetSource).shift(subRowSetToShift, delta);
        if (timestampColumnName != null) {
            ((LongSparseArraySource) startSource).shift(subRowSetToShift, delta);
            ((LongSparseArraySource) endSource).shift(subRowSetToShift, delta);
        }
    }
    // endregion Shifts

    // region clear-output
    @Override
    public void clearOutputRows(final RowSet toClear) {
        // if we are redirected, clear the inner source
        if (rowRedirection != null) {
            ChunkUtils.fillWithNullValue(innerGroupRowSetSource, toClear);
        } else {
            ChunkUtils.fillWithNullValue(groupRowSetSource, toClear);
        }
    }
    // endregion clear-output


    @Override
    public void startTrackingPrev() {
        groupRowSetSource.startTrackingPrevValues();
        if (rowRedirection != null) {
            assert innerGroupRowSetSource != null;
            innerGroupRowSetSource.startTrackingPrevValues();
        }

        // If we are time-based, track the start/end sources as well
        if (timestampColumnName != null) {
            startSource.startTrackingPrevValues();
            endSource.startTrackingPrevValues();
            if (rowRedirection != null) {
                assert innerStartSource != null;
                innerStartSource.startTrackingPrevValues();
                assert innerEndSource != null;
                innerEndSource.startTrackingPrevValues();
            }
        }
        for (ColumnSource<?> outputSource : outputSources) {
            outputSource.startTrackingPrevValues();
        }
    }

    /**
     * Get an array of the output column names.
     *
     * @return the output column names.
     */
    @NotNull
    protected String[] getOutputColumnNames() {
        return outputColumnNames;
    }

    /**
     * Get a map of outputName to output {@link ColumnSource} for this operation.
     *
     * @return a map of output column name to output column source
     */
    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return outputSourceMap;
    }

    /**
     * Get the names of the input column(s) for this operator.
     *
     * @return the names of the input column
     */
    @NotNull
    @Override
    protected String[] getInputColumnNames() {
        return inputColumnNames;
    }

    /**
     * Return whether the operator needs affected and influencer row positions during accumulation. RollingGroup sets
     * this to {@code true}.
     */
    @Override
    protected boolean requiresRowPositions() {
        return true;
    }

    /**
     * Create the modified column set for the input columns of this operator.
     */
    @Override
    protected void createInputModifiedColumnSet(@NotNull final QueryTable source) {
        inputModifiedColumnSet = source.newModifiedColumnSet(getAffectingColumnNames());
        // inputModifiedColumnSet needs to be set before we can create the transformer.
        createInputOutputTransformer();
    }

    /**
     * Create the modified column set for the output columns from this operator.
     */
    @Override
    protected void createOutputModifiedColumnSet(@NotNull final QueryTable result) {
        final String[] colNames = getOutputColumnNames();
        outputModifiedColumnSet = result.newModifiedColumnSet(colNames);

        // Create an individual MCS for each output column.
        outputModifiedColumnSets = new ModifiedColumnSet[colNames.length];
        for (int ii = 0; ii < colNames.length; ii++) {
            outputModifiedColumnSets[ii] = result.newModifiedColumnSet(colNames[ii]);
        }
        // outputModifiedColumnSets need to be set before we can create the transformer.
        createInputOutputTransformer();
    }

    private void createInputOutputTransformer() {
        if (inputOutputTransformer != null || inputModifiedColumnSet == null || outputModifiedColumnSet == null) {
            return;
        }
        // Create the transformer to map from the input columns to the individual output column MCS.
        inputOutputTransformer =
                inputModifiedColumnSet.newTransformer(getInputColumnNames(), outputModifiedColumnSets);
    }

    /**
     * Set the downstream modified column set appropriately for this operator.
     */
    @Override
    protected void extractDownstreamModifiedColumnSet(@NotNull final TableUpdate upstream,
            @NotNull final TableUpdate downstream) {
        if (upstream.added().isNonempty() || upstream.removed().isNonempty()) {
            downstream.modifiedColumnSet().setAll(getOutputModifiedColumnSet());
            return;
        }

        if (upstream.modified().isNonempty()) {
            inputOutputTransformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet());
        }
    }
}
