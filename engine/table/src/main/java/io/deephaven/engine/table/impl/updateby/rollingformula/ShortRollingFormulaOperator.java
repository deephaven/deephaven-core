//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollingFormulaOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingformula;

import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.ShortRingBufferVectorWrapper;
import io.deephaven.vector.ShortVector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.function.IntConsumer;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Rolling formula operator for source short columns. The output column type will be entirely dependent on the formula
 * provided by the user.
 */

public class ShortRollingFormulaOperator extends BaseRollingFormulaOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseRollingFormulaOperator.Context {
        private final IntConsumer outputSetter;

        private ShortChunk<? extends Values> influencerValuesChunk;
        private ShortRingBuffer shortWindowValues;

        @SuppressWarnings("unchecked")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize, influencerChunkSize);

            shortWindowValues = new ShortRingBuffer(BUFFER_INITIAL_CAPACITY, true);

            // Make a copy of the operator formula column.
            final FormulaColumn formulaCopy = (FormulaColumn) formulaColumn.copy();

            // Create a single value column source of the appropriate type for the formula column input.
            final SingleValueColumnSource<ShortVector> formulaInputSource =
                    (SingleValueColumnSource<ShortVector>) SingleValueColumnSource
                            .getSingleValueColumnSource(inputVectorType);
            formulaInputSource.set(new ShortRingBufferVectorWrapper(shortWindowValues));
            formulaCopy.initInputs(RowSetFactory.flat(1).toTracking(),
                    Collections.singletonMap(PARAM_COLUMN_NAME, formulaInputSource));

            final ColumnSource<?> formulaOutputSource =
                    ReinterpretUtils.maybeConvertToPrimitive(formulaCopy.getDataView());
            outputSetter = getChunkSetter(outputValues, formulaOutputSource);
        }

        @Override
        public void close() {
            super.close();
            shortWindowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asShortChunk();
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
            shortWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final short val = influencerValuesChunk.get(pos + ii);
                shortWindowValues.addUnsafe(val);
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(shortWindowValues.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                shortWindowValues.removeUnsafe();
            }
        }

        @Override
        public void reset() {
            shortWindowValues.clear();
        }
    }

    public ShortRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String formula,
            @NotNull final String paramToken,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                formula,
                paramToken,
                formulaColumnMap,
                tableDef,
                compilationProcessor);
        // region constructor
        // endregion constructor
    }

    protected ShortRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final Class<?> columnType,
            final Class<?> componentType,
            final Class<?> vectorType,
            @NotNull final Map<Class<?>, FormulaColumn> formulaColumnMap,
            @NotNull final TableDefinition tableDef
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
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
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new ShortRollingFormulaOperator(pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                inputColumnType,
                inputComponentType,
                inputVectorType,
                formulaColumnMap,
                tableDef
        // region extra-copy-args
        // endregion extra-copy-args
        );
    }

    @Override
    public UpdateByOperator.@NotNull Context makeUpdateContext(int affectedChunkSize, int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}
