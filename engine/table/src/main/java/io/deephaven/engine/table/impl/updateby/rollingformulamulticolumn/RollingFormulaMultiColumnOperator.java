//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn;

import io.deephaven.base.ringbuffer.*;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.RingBufferVectorWrapper;
import io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer.RingBufferWindowConsumer;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.IntConsumer;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class RollingFormulaMultiColumnOperator extends UpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 512;

    private final TableDefinition tableDef;
    private final SelectColumn selectColumn;
    private final String[] inputColumnNames;
    private final Class<?>[] inputColumnTypes;
    private final Class<?>[] inputComponentTypes;
    private final Class<?>[] inputVectorTypes;

    private WritableColumnSource<?> primitiveOutputSource;
    private WritableColumnSource<?> outputSource;
    private WritableColumnSource<?> maybeInnerSource;
    private ChunkType outputChunkType;

    private class Context extends UpdateByOperator.Context {
        private final ChunkSink.FillFromContext outputFillContext;
        private final WritableChunk<? extends Values> outputValues;

        private final IntConsumer outputSetter;

        private final RingBufferWindowConsumer[] inputConsumers;

        @SuppressWarnings("unused")
        private Context(final int affectedChunkSize, final int influencerChunkSize) {
            outputFillContext = primitiveOutputSource.makeFillFromContext(affectedChunkSize);
            outputValues = outputChunkType.makeWritableChunk(affectedChunkSize);

            // Make a copy of the operator formula column.
            final SelectColumn contextSelectColumn = selectColumn.copy();

            inputConsumers = new RingBufferWindowConsumer[inputColumnNames.length];

            // To perform the calculation, we will leverage SelectColumn and for its input sources we create a set of
            // SingleValueColumnSources, each containing a Vector of values. This vector will contain exactly the
            // values from the input columns that are appropriate for output row given the window configuration.
            // The formula column is evaluated once per output row and the result written to the output column
            // source.

            // The SingleValueColumnSources is backed by RingBuffers through use of a RingBufferVectorWrapper.
            // The underlying RingBuffer is updated with the values from the input columns with assistance from
            // the RingBufferWindowConsumer class, which abstracts the process of pushing and popping values from input
            // column data chunks into the RingBuffer.

            final Map<String, ColumnSource<?>> inputSources = new HashMap<>();
            for (int i = 0; i < inputColumnNames.length; i++) {
                final String inputColumnName = inputColumnNames[i];
                final Class<?> inputColumnType = inputColumnTypes[i];
                final Class<?> inputVectorType = inputVectorTypes[i];
                final Class<?> inputComponentType = inputComponentTypes[i];

                // Create and store the ring buffer for the input column.
                final RingBuffer ringBuffer = RingBuffer.makeRingBuffer(
                        inputColumnType,
                        BUFFER_INITIAL_CAPACITY,
                        true);

                // Create a single value column source wrapping the ring buffer.
                // noinspection unchecked
                final SingleValueColumnSource<Vector<?>> formulaInputSource =
                        (SingleValueColumnSource<Vector<?>>) SingleValueColumnSource
                                .getSingleValueColumnSource(inputVectorType);

                final RingBufferVectorWrapper<?> wrapper = RingBufferVectorWrapper.makeRingBufferVectorWrapper(
                        ringBuffer,
                        inputComponentType);

                formulaInputSource.set(wrapper);

                inputSources.put(inputColumnName, formulaInputSource);

                inputConsumers[i] = RingBufferWindowConsumer.create(ringBuffer);
            }
            contextSelectColumn.initInputs(RowSetFactory.flat(1).toTracking(), inputSources);

            final ColumnSource<?> formulaOutputSource =
                    ReinterpretUtils.maybeConvertToPrimitive(contextSelectColumn.getDataView());
            outputSetter = getChunkSetter(outputValues, formulaOutputSource);
        }

        @Override
        protected void setValueChunks(@NotNull Chunk<? extends Values>[] valueChunks) {
            // Assign the influencer values chunks to the input consumers.
            for (int i = 0; i < valueChunks.length; i++) {
                inputConsumers[i].setInputChunk(valueChunks[i]);
            }
        }

        @Override
        protected void push(int pos, int count) {
            throw new IllegalStateException("RollingFormulaMultiColumnOperator.Context.push should never be called.");
        }

        @Override
        public void accumulateCumulative(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @Nullable final LongChunk<? extends Values> tsChunk,
                final int len) {
            throw new UnsupportedOperationException("RollingFormula is not supported in cumulative operations.");
        }

        @Override
        public void accumulateRolling(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] influencerValueChunkArr,
                @Nullable final LongChunk<OrderedRowKeys> affectedPosChunk,
                @Nullable final LongChunk<OrderedRowKeys> influencerPosChunk,
                @NotNull final IntChunk<? extends Values> pushChunk,
                @NotNull final IntChunk<? extends Values> popChunk,
                final int len) {

            setValueChunks(influencerValueChunkArr);
            setPosChunks(affectedPosChunk, influencerPosChunk);

            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                final int pushCount = pushChunk.get(ii);
                final int popCount = popChunk.get(ii);

                if (pushCount == NULL_INT) {
                    outputValues.fillWithNullValue(ii, 1);
                    continue;
                }

                // pop for this row
                if (popCount > 0) {
                    for (RingBufferWindowConsumer consumer : inputConsumers) {
                        consumer.pop(popCount);
                    }
                }

                // push for this row
                if (pushCount > 0) {
                    for (RingBufferWindowConsumer consumer : inputConsumers) {
                        consumer.push(pushIndex, pushCount);
                    }
                    pushIndex += pushCount;
                }

                // If not empty (even if completely full of null), run the formula over the window values.
                outputSetter.accept(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        protected void writeToOutputChunk(int outIdx) {
            throw new IllegalStateException(
                    "RollingFormulaMultiColumnOperator.Context.writeToOutputChunk should never be called.");
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            primitiveOutputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
        }

        @Override
        public void reset() {
            // Clear all the ring buffers for re-use
            for (RingBufferWindowConsumer consumer : inputConsumers) {
                consumer.reset();
            }
            nullCount = 0;
        }

        @Override
        public void close() {
            outputValues.close();
            outputFillContext.close();
        }
    }

    /**
     * Create a new RollingFormulaMultiColumnOperator.
     *
     * @param pair Contains the output column name as a MatchPair
     * @param affectingColumns The names of the columns that when changed would affect this formula output
     * @param timestampColumnName The name of the column containing timestamps for time-based calculations (or null when
     *        not time-based)
     * @param reverseWindowScaleUnits The size of the reverse window in ticks (or nanoseconds when time-based)
     * @param forwardWindowScaleUnits The size of the forward window in ticks (or nanoseconds when time-based)
     * @param tableDef The table definition for the table containing the columns
     * @param selectColumn The {@link SelectColumn} specifying the calculation to be performed
     * @param inputColumnNames The names of the columns to be used as inputs
     * @param inputColumnTypes The types of the columns to be used as inputs
     * @param inputComponentTypes The component types of the columns to be used as inputs
     * @param inputVectorTypes The vector types of the columns to be used as inputs
     */
    public RollingFormulaMultiColumnOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final TableDefinition tableDef,
            @NotNull final SelectColumn selectColumn,
            @NotNull final String[] inputColumnNames,
            @NotNull final Class<?>[] inputColumnTypes,
            @NotNull final Class<?>[] inputComponentTypes,
            @NotNull final Class<?>[] inputVectorTypes) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.tableDef = tableDef;
        this.selectColumn = selectColumn;
        this.inputColumnNames = inputColumnNames;
        this.inputColumnTypes = inputColumnTypes;
        this.inputComponentTypes = inputComponentTypes;
        this.inputVectorTypes = inputVectorTypes;
    }

    @Override
    public UpdateByOperator copy() {
        return new RollingFormulaMultiColumnOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                tableDef,
                selectColumn,
                inputColumnNames,
                inputColumnTypes,
                inputComponentTypes,
                inputVectorTypes);
    }

    @Override
    public void initializeSources(@NotNull final Table source, @Nullable final RowRedirection rowRedirection) {
        this.rowRedirection = rowRedirection;

        if (rowRedirection != null) {
            // region create-dense
            maybeInnerSource = ArrayBackedColumnSource.getMemoryColumnSource(0, selectColumn.getReturnedType());
            // endregion create-dense
            outputSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
        } else {
            maybeInnerSource = null;
            // region create-sparse
            outputSource = SparseArrayColumnSource.getSparseMemoryColumnSource(0, selectColumn.getReturnedType());
            // endregion create-sparse
        }

        primitiveOutputSource = ReinterpretUtils.maybeConvertToWritablePrimitive(outputSource);

        outputChunkType = primitiveOutputSource.getChunkType();
    }

    protected static IntConsumer getChunkSetter(
            final WritableChunk<? extends Values> valueChunk,
            final ColumnSource<?> formulaOutputSource) {
        final ChunkType chunkType = valueChunk.getChunkType();
        switch (chunkType) {
            case Boolean:
                throw new IllegalStateException(
                        "Output chunk type should not be Boolean but should have been reinterpreted to byte");
            case Byte:
                final WritableByteChunk<? extends Values> byteChunk = valueChunk.asWritableByteChunk();
                return index -> byteChunk.set(index, formulaOutputSource.getByte(0));

            case Char:
                final WritableCharChunk<? extends Values> charChunk = valueChunk.asWritableCharChunk();
                return index -> charChunk.set(index, formulaOutputSource.getChar(0));

            case Double:
                final WritableDoubleChunk<? extends Values> doubleChunk = valueChunk.asWritableDoubleChunk();
                return index -> doubleChunk.set(index, formulaOutputSource.getDouble(0));

            case Float:
                final WritableFloatChunk<? extends Values> floatChunk = valueChunk.asWritableFloatChunk();
                return index -> floatChunk.set(index, formulaOutputSource.getFloat(0));

            case Int:
                final WritableIntChunk<? extends Values> intChunk = valueChunk.asWritableIntChunk();
                return index -> intChunk.set(index, formulaOutputSource.getInt(0));

            case Long:
                final WritableLongChunk<? extends Values> longChunk = valueChunk.asWritableLongChunk();
                return index -> longChunk.set(index, formulaOutputSource.getLong(0));

            case Short:
                final WritableShortChunk<? extends Values> shortChunk = valueChunk.asWritableShortChunk();
                return index -> shortChunk.set(index, formulaOutputSource.getShort(0));

            default:
                final WritableObjectChunk<Object, ? extends Values> objectChunk = valueChunk.asWritableObjectChunk();
                return index -> {
                    Object result = formulaOutputSource.get(0);
                    if (result instanceof RingBufferVectorWrapper) {
                        // Handle the rare (and probably not useful) case where the formula is an identity. We need to
                        // copy the data in the RingBuffer and store that as a DirectVector. If not, we will point to
                        // the
                        // live data in the ring.
                        result = ((Vector<?>) result).getDirect();
                    }
                    objectChunk.set(index, result);
                };
        }
    }

    @Override
    public void startTrackingPrev() {
        outputSource.startTrackingPrevValues();
        if (rowRedirection != null) {
            assert maybeInnerSource != null;
            maybeInnerSource.startTrackingPrevValues();
        }
    }

    @Override
    public UpdateByOperator.@NotNull Context makeUpdateContext(int affectedChunkSize, int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        if (rowRedirection != null) {
            assert maybeInnerSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) maybeInnerSource)
                    .prepareForParallelPopulation(changedRows);
        } else {
            ((WritableSourceWithPrepareForParallelPopulation) outputSource).prepareForParallelPopulation(changedRows);
        }
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }

    // region clear-output
    @Override
    public void clearOutputRows(final RowSet toClear) {
        // if we are redirected, clear the inner source
        if (rowRedirection != null) {
            ChunkUtils.fillWithNullValue(maybeInnerSource, toClear);
        } else {
            ChunkUtils.fillWithNullValue(outputSource, toClear);
        }
    }

    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((SparseArrayColumnSource<?>) outputSource).shift(subRowSetToShift, delta);
    }

    @Override
    @NotNull
    protected String[] getInputColumnNames() {
        return inputColumnNames;
    }
}
