package io.deephaven.engine.table.impl.updateby.rollingformula;

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
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.deephaven.util.QueryConstants.*;

abstract class BaseRollingFormulaOperator extends UpdateByOperator {
    protected final String PARAM_COLUMN_NAME = "__PARAM_COLUMN__";

    @NotNull
    final Map<Class<?>, FormulaColumn> formulaColumnMap;
    @NotNull
    final TableDefinition tableDef;

    final FormulaColumn formulaColumn;
    final Class<?> vectorType;

    protected WritableColumnSource<?> outputSource;
    protected WritableColumnSource<?> maybeInnerSource;
    ChunkType outputChunkType;
    Object nullValue;

    abstract class Context extends UpdateByOperator.Context {
        protected final ChunkSink.FillFromContext outputFillContext;
        final WritableChunk<? extends Values> outputValues;
        final BiConsumer<Object, Integer> outputSetter;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            outputFillContext = outputSource.makeFillFromContext(affectedChunkSize);
            outputValues = outputChunkType.makeWritableChunk(affectedChunkSize);
            outputSetter = getChunkSetter(outputValues);
        }

        @Override
        public void accumulateCumulative(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @Nullable final LongChunk<? extends Values> tsChunk,
                final int len) {
            throw new UnsupportedOperationException("RollingFormula is not supported in cumulative operations.");
        }

        void writeNullToOutputChunk(final int outIdx) {
            outputSetter.accept(nullValue, outIdx);
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
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

        final Class<?> columnType = tableDef.getColumn(pair.rightColumn).getDataType();
        vectorType = getVectorType(columnType);

        // Handle the rare (and probably not useful) case where the formula is an identity formula. We need to make
        // a copy of the RingBuffer wrapper and store that as a DirectVector. If not, we will point to the live data
        // in the ring.
        final String formulaToUse = formula.equals(paramToken) ? formula + ".getDirect()" : formula;

        // Re-use the formula column if it's already been created for this type. No need to synchronize; these
        // operators are created serially.
        // TODO: does generic Object need to be handled uniquely?
        formulaColumn = formulaColumnMap.computeIfAbsent(columnType, t -> {
            final FormulaColumn tmp = FormulaColumn.createFormulaColumn(outputColumnName,
                    FormulaUtil.replaceFormulaTokens(formulaToUse, paramToken, PARAM_COLUMN_NAME));

            final ColumnDefinition<?> inputColumnDefinition = ColumnDefinition
                    .fromGenericType(PARAM_COLUMN_NAME, vectorType, columnType);
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
            final Class<?> vectorType,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.formulaColumnMap = formulaColumnMap;
        this.tableDef = tableDef;

        final Class<?> columnType = tableDef.getColumn(pair.rightColumn).getDataType();
        this.vectorType = vectorType;

        // Re-use the formula column that already been created for this type.
        // TODO: does generic Object need to be handled uniquely?
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
        outputChunkType = outputSource.getChunkType();
        nullValue = getNullValue(outputChunkType);
    }

    private static Class<?> getVectorType(final Class<?> type) {
        if (type == Boolean.class) {
            return ObjectVector.class;
        }
        if (type == byte.class) {
            return ByteVector.class;
        }
        if (type == char.class) {
            return CharVector.class;
        }
        if (type == double.class) {
            return DoubleVector.class;
        }
        if (type == float.class) {
            return FloatVector.class;
        }
        if (type == int.class) {
            return IntVector.class;
        }
        if (type == long.class) {
            return LongVector.class;
        }
        if (type == short.class) {
            return ShortVector.class;
        }
        return ObjectVector.class;
    }

    private static BiConsumer<Object, Integer> getChunkSetter(WritableChunk<? extends Values> valueChunk) {
        final ChunkType chunkType = valueChunk.getChunkType();
        if (chunkType == ChunkType.Boolean) {
            return (o, i) -> valueChunk.asWritableBooleanChunk().set(i, (Boolean) o);
        }
        if (chunkType == ChunkType.Byte) {
            return (o, i) -> valueChunk.asWritableByteChunk().set(i, o == null ? NULL_BYTE : (byte) o);
        }
        if (chunkType == ChunkType.Char) {
            return (o, i) -> valueChunk.asWritableCharChunk().set(i, o == null ? NULL_CHAR : (char) o);
        }
        if (chunkType == ChunkType.Double) {
            return (o, i) -> valueChunk.asWritableDoubleChunk().set(i, o == null ? NULL_DOUBLE : (double) o);
        }
        if (chunkType == ChunkType.Float) {
            return (o, i) -> valueChunk.asWritableFloatChunk().set(i, o == null ? NULL_FLOAT : (float) o);
        }
        if (chunkType == ChunkType.Int) {
            return (o, i) -> valueChunk.asWritableIntChunk().set(i, o == null ? NULL_INT : (int) o);
        }
        if (chunkType == ChunkType.Long) {
            return (o, i) -> valueChunk.asWritableLongChunk().set(i, o == null ? NULL_LONG : (long) o);
        }
        if (chunkType == ChunkType.Short) {
            return (o, i) -> valueChunk.asWritableShortChunk().set(i, o == null ? NULL_SHORT : (short) o);
        }
        return (o, i) -> valueChunk.asWritableObjectChunk().set(i, o);
    }

    private static Object getNullValue(final ChunkType chunkType) {
        if (chunkType == ChunkType.Boolean) {
            return null;
        }
        if (chunkType == ChunkType.Byte) {
            return NULL_BYTE;
        }
        if (chunkType == ChunkType.Char) {
            return NULL_CHAR;
        }
        if (chunkType == ChunkType.Double) {
            return NULL_DOUBLE;
        }
        if (chunkType == ChunkType.Float) {
            return NULL_FLOAT;
        }
        if (chunkType == ChunkType.Int) {
            return NULL_INT;
        }
        if (chunkType == ChunkType.Long) {
            return NULL_LONG;
        }
        return null;
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
