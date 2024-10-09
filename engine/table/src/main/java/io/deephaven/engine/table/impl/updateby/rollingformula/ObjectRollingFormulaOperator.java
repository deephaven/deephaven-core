//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingformula;

import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.ObjectRingBufferVectorWrapper;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.function.IntConsumer;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Rolling formula operator for source char columns. The output column type will be entirely dependent on the formula
 * provided by the user.
 */

public class ObjectRollingFormulaOperator<T> extends BaseRollingFormulaOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseRollingFormulaOperator.Context {
        private final IntConsumer outputSetter;

        private ObjectChunk<T, ? extends Values> influencerValuesChunk;
        private ObjectRingBuffer<T> windowValues;

        @SuppressWarnings("unchecked")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize, influencerChunkSize);

            windowValues = new ObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, true);

            // Make a copy of the operator formula column.
            final FormulaColumn formulaCopy = (FormulaColumn) formulaColumn.copy();

            // Create a single value column source of the appropriate type for the formula column input.
            final SingleValueColumnSource<ObjectVector<?>> formulaInputSource =
                    (SingleValueColumnSource<ObjectVector<?>>) SingleValueColumnSource
                            .getSingleValueColumnSource(inputVectorType);
            // noinspection rawtypes
            formulaInputSource.set(new ObjectRingBufferVectorWrapper(windowValues, inputComponentType));
            formulaCopy.initInputs(RowSetFactory.flat(1).toTracking(),
                    Collections.singletonMap(PARAM_COLUMN_NAME, formulaInputSource));

            outputSetter = getChunkSetter(outputValues, formulaCopy.getDataView());
        }

        @Override
        public void close() {
            super.close();
            windowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asObjectChunk();
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
                    pop(popCount);
                }

                // push for this row
                if (pushCount > 0) {
                    push(pushIndex, pushCount);
                    pushIndex += pushCount;
                }

                // If not empty (even if completely full of null), run the formula over the window values.
                outputSetter.accept(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final T val = influencerValuesChunk.get(pos + ii);
                windowValues.addUnsafe(val);
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowValues.size(), "charWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                windowValues.removeUnsafe();
            }
        }

        @Override
        public void reset() {
            windowValues.clear();
        }
    }


    public ObjectRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String formula,
            @NotNull final String paramToken,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, formula,
                paramToken, formulaColumnMap, tableDef, compilationProcessor);
    }

    protected ObjectRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final Class<?> columnType,
            final Class<?> componentType,
            final Class<?> vectorType,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef) {
        super(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                columnType,
                componentType,
                vectorType,
                formulaColumnMap,
                tableDef);
    }

    @Override
    public UpdateByOperator copy() {
        return new ObjectRollingFormulaOperator<>(pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                inputColumnType,
                inputComponentType,
                inputVectorType,
                formulaColumnMap,
                tableDef);
    }

    @Override
    public UpdateByOperator.@NotNull Context makeUpdateContext(int affectedChunkSize, int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}
