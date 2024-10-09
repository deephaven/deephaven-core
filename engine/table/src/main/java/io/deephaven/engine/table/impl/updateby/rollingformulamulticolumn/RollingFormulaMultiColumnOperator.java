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
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.RingBufferVectorWrapper;
import io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.windowconsumer.RingBufferWindowConsumer;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.vector.Vector;
import io.deephaven.vector.VectorFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.IntConsumer;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class RollingFormulaMultiColumnOperator extends UpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 512;

    private final String formula;
    private final TableDefinition tableDef;
    private final QueryCompilerRequestProcessor compilationProcessor;
    private final FormulaColumn formulaColumn;
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
            final FormulaColumn formulaCopy = (FormulaColumn) formulaColumn.copy();

            inputConsumers = new RingBufferWindowConsumer[inputColumnNames.length];

            // To perform the calculation, we will leverage FormulaColumn and for its input sources we create a set of
            // SingleValueColumnSources, each containing a Vector of values. This vector will contain exactly the
            // values from the input columns that are appropriate for output row given the RollingWindow configuration.
            // The formula column will be evaluated once per output row and the result written to the output column
            // source.

            // The SingleValueColumnSources will be backed by RingBuffers through use of a RingBufferVectorWrapper.
            // The underlying RingBuffer will be updated with the values from the input columns with assistance from
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
            formulaCopy.initInputs(RowSetFactory.flat(1).toTracking(), inputSources);

            final ColumnSource<?> formulaOutputSource =
                    ReinterpretUtils.maybeConvertToPrimitive(formulaCopy.getDataView());
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
     * @param formula The formula specifying the calculation to be performed
     * @param reverseWindowScaleUnits The size of the reverse window in ticks (or nanoseconds when time-based)
     * @param forwardWindowScaleUnits The size of the forward window in ticks (or nanoseconds when time-based)
     * @param tableDef The table definition for the table containing the columns
     * @param compilationProcessor The shared {@link QueryCompilerRequestProcessor} to use for formula compilation
     */
    public RollingFormulaMultiColumnOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            @NotNull final String formula,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final TableDefinition tableDef,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);

        this.formula = formula;
        this.tableDef = tableDef;
        this.compilationProcessor = compilationProcessor;

        final String outputColumnName = pair.leftColumn;

        formulaColumn = FormulaColumn.createFormulaColumn(outputColumnName, formula);

        final Map<String, ColumnDefinition<?>> columnDefinitionMap = tableDef.getColumnNameMap();

        // Create a column definition map composed of vector types for the formula.
        final Map<String, ColumnDefinition<?>> vectorColumnNameMap = new HashMap<>();
        columnDefinitionMap.forEach((key, value) -> {
            final ColumnDefinition<?> columnDef = ColumnDefinition.fromGenericType(
                    key, VectorFactory.forElementType(value.getDataType()).vectorType());
            vectorColumnNameMap.put(key, columnDef);
        });

        // Get the input column names and data types from the formula.
        inputColumnNames = formulaColumn.initDef(vectorColumnNameMap, compilationProcessor).toArray(String[]::new);
        inputColumnTypes = new Class[inputColumnNames.length];
        inputComponentTypes = new Class[inputColumnNames.length];
        inputVectorTypes = new Class[inputColumnNames.length];

        for (int i = 0; i < inputColumnNames.length; i++) {
            final ColumnDefinition<?> columnDef = columnDefinitionMap.get(inputColumnNames[i]);
            inputColumnTypes[i] = columnDef.getDataType();
            inputComponentTypes[i] = columnDef.getComponentType();
            inputVectorTypes[i] = vectorColumnNameMap.get(inputColumnNames[i]).getDataType();
        }
    }

    @Override
    public UpdateByOperator copy() {
        return new RollingFormulaMultiColumnOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                formula,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                tableDef,
                compilationProcessor);
    }

    @Override
    public void initializeSources(@NotNull final Table source, @Nullable final RowRedirection rowRedirection) {
        this.rowRedirection = rowRedirection;

        if (rowRedirection != null) {
            // region create-dense
            maybeInnerSource = ArrayBackedColumnSource.getMemoryColumnSource(0, formulaColumn.getReturnedType());
            // endregion create-dense
            outputSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
        } else {
            maybeInnerSource = null;
            // region create-sparse
            outputSource = SparseArrayColumnSource.getSparseMemoryColumnSource(0, formulaColumn.getReturnedType());
            // endregion create-sparse
        }

        primitiveOutputSource = ReinterpretUtils.maybeConvertToWritablePrimitive(outputSource);

        outputChunkType = primitiveOutputSource.getChunkType();
    }

    protected static IntConsumer getChunkSetter(
            final WritableChunk<? extends Values> valueChunk,
            final ColumnSource<?> formulaOutputSource) {
        final ChunkType chunkType = valueChunk.getChunkType();
        if (chunkType == ChunkType.Boolean) {
            throw new IllegalStateException(
                    "Output chunk type should not be Boolean but should have been reinterpreted to byte");
        }
        if (chunkType == ChunkType.Byte) {
            final WritableByteChunk<? extends Values> writableChunk = valueChunk.asWritableByteChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getByte(0));
        }
        if (chunkType == ChunkType.Char) {
            final WritableCharChunk<? extends Values> writableChunk = valueChunk.asWritableCharChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getChar(0));
        }
        if (chunkType == ChunkType.Double) {
            final WritableDoubleChunk<? extends Values> writableChunk = valueChunk.asWritableDoubleChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getDouble(0));
        }
        if (chunkType == ChunkType.Float) {
            final WritableFloatChunk<? extends Values> writableChunk = valueChunk.asWritableFloatChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getFloat(0));
        }
        if (chunkType == ChunkType.Int) {
            final WritableIntChunk<? extends Values> writableChunk = valueChunk.asWritableIntChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getInt(0));
        }
        if (chunkType == ChunkType.Long) {
            final WritableLongChunk<? extends Values> writableChunk = valueChunk.asWritableLongChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getLong(0));
        }
        if (chunkType == ChunkType.Short) {
            final WritableShortChunk<? extends Values> writableChunk = valueChunk.asWritableShortChunk();
            return index -> writableChunk.set(index, formulaOutputSource.getShort(0));
        }
        final WritableObjectChunk<Object, ? extends Values> writableChunk = valueChunk.asWritableObjectChunk();
        return index -> {
            Object result = formulaOutputSource.get(0);
            if (result instanceof RingBufferVectorWrapper) {
                // Handle the rare (and probably not useful) case where the formula is an identity. We need to
                // copy the data in the RingBuffer and store that as a DirectVector. If not, we will point to the
                // live data in the ring.
                result = ((Vector<?>) result).getDirect();
            }
            writableChunk.set(index, result);
        };
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
    protected String[] getAffectingColumnNames() {
        return ArrayUtils.addAll(affectingColumns, inputColumnNames);
    }

    @Override
    @NotNull
    protected String[] getInputColumnNames() {
        return inputColumnNames;
    }
}
