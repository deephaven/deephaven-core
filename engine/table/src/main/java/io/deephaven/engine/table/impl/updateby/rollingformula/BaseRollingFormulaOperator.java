package io.deephaven.engine.table.impl.updateby.rollingformula;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.FormulaUtil;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.RingBufferVectorWrapper;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.function.IntConsumer;

abstract class BaseRollingFormulaOperator extends UpdateByOperator {
    protected final String PARAM_COLUMN_NAME = "__PARAM_COLUMN__";

    @NotNull
    final Map<Class<?>, FormulaColumn> formulaColumnMap;
    @NotNull
    final TableDefinition tableDef;

    final FormulaColumn formulaColumn;
    final Class<?> inputVectorType;

    protected WritableColumnSource<?> primitiveOutputSource;
    protected WritableColumnSource<?> outputSource;
    protected WritableColumnSource<?> maybeInnerSource;
    ChunkType outputChunkType;

    abstract class Context extends UpdateByOperator.Context {
        protected final ChunkSink.FillFromContext outputFillContext;
        final WritableChunk<? extends Values> outputValues;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            outputFillContext = primitiveOutputSource.makeFillFromContext(affectedChunkSize);
            outputValues = outputChunkType.makeWritableChunk(affectedChunkSize);
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
        protected void writeToOutputChunk(int outIdx) {
            throw Assert.statementNeverExecuted("RollingFormulaOperator.Context.writeToOutputChunk");
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            primitiveOutputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
        }

        @Override
        public void reset() {
            nullCount = 0;
        }

        @Override
        public void close() {
            outputValues.close();
            outputFillContext.close();
        }
    }

    public BaseRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String formula,
            @NotNull final String paramToken,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.formulaColumnMap = formulaColumnMap;
        this.tableDef = tableDef;

        final String outputColumnName = pair.leftColumn;

        final Class<?> inputColumnType = tableDef.getColumn(pair.rightColumn).getDataType();
        inputVectorType = getVectorType(inputColumnType);

        // Re-use the formula column if it's already been created for this type. No need to synchronize; these
        // operators are created serially.
        formulaColumn = formulaColumnMap.computeIfAbsent(inputColumnType, t -> {
            final FormulaColumn tmp = FormulaColumn.createFormulaColumn(outputColumnName,
                    FormulaUtil.replaceFormulaTokens(formula, paramToken, PARAM_COLUMN_NAME));

            final ColumnDefinition<?> inputColumnDefinition = ColumnDefinition
                    .fromGenericType(PARAM_COLUMN_NAME, inputVectorType, inputColumnType);
            tmp.initDef(Collections.singletonMap(PARAM_COLUMN_NAME, inputColumnDefinition));
            return tmp;
        });
    }

    protected BaseRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final Class<?> inputVectorType,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.formulaColumnMap = formulaColumnMap;
        this.tableDef = tableDef;

        final Class<?> columnType = tableDef.getColumn(pair.rightColumn).getDataType();
        this.inputVectorType = inputVectorType;

        // Re-use the formula column already created for this type.
        formulaColumn = formulaColumnMap.computeIfAbsent(columnType, t -> {
            throw new IllegalStateException("formulaColumnMap should have been populated for " + columnType);
        });
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

    private static Class<?> getVectorType(final Class<?> type) {
        if (type == byte.class || type == Byte.class) {
            return ByteVector.class;
        }
        if (type == char.class || type == Character.class) {
            return CharVector.class;
        }
        if (type == double.class || type == Double.class) {
            return DoubleVector.class;
        }
        if (type == float.class || type == Float.class) {
            return FloatVector.class;
        }
        if (type == int.class || type == Integer.class) {
            return IntVector.class;
        }
        if (type == long.class || type == Long.class) {
            return LongVector.class;
        }
        if (type == short.class || type == Short.class) {
            return ShortVector.class;
        }
        return ObjectVector.class;
    }

    protected static IntConsumer getChunkSetter(
            final WritableChunk<? extends Values> valueChunk,
            final ColumnSource<?> formulaOutputSource) {
        final ChunkType chunkType = valueChunk.getChunkType();
        if (chunkType == ChunkType.Boolean) {
            throw new IllegalStateException("Output chunk type should not be Boolean but should have been reinterpreted to byte");
        }
        if (chunkType == ChunkType.Byte) {
            return i -> valueChunk.asWritableByteChunk().set(i, formulaOutputSource.getByte(0));
        }
        if (chunkType == ChunkType.Char) {
            return i -> valueChunk.asWritableCharChunk().set(i, formulaOutputSource.getChar(0));
        }
        if (chunkType == ChunkType.Double) {
            return i -> valueChunk.asWritableDoubleChunk().set(i, formulaOutputSource.getDouble(0));
        }
        if (chunkType == ChunkType.Float) {
            return i -> valueChunk.asWritableFloatChunk().set(i, formulaOutputSource.getFloat(0));
        }
        if (chunkType == ChunkType.Int) {
            return i -> valueChunk.asWritableIntChunk().set(i, formulaOutputSource.getInt(0));
        }
        if (chunkType == ChunkType.Long) {
            return i -> valueChunk.asWritableLongChunk().set(i, formulaOutputSource.getLong(0));
        }
        if (chunkType == ChunkType.Short) {
            return i -> valueChunk.asWritableShortChunk().set(i, formulaOutputSource.getShort(0));
        }
        return i -> {
            Object result = formulaOutputSource.get(0);
            if (result instanceof RingBufferVectorWrapper) {
                // Handle the rare (and probably not useful) case where the formula is an identity. We need to
                // copy the data in the RingBuffer and store that as a DirectVector. If not, we will point to the
                // live data in the ring.
                result = ((Vector<?>) result).getDirect();
            }
            valueChunk.asWritableObjectChunk().set(i, result);
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
}
