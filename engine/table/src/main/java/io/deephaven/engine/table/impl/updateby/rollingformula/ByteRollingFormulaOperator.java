/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingFormulaOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingformula;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.ByteRingBufferVectorWrapper;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.vector.ByteVector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Rolling formula operator for source byte columns. The output column type will be entirely dependent on the formula
 * provided by the user.
 */

public class ByteRollingFormulaOperator extends BaseRollingFormulaOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;

    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseRollingFormulaOperator.Context {
        private final SingleValueColumnSource<ByteVector> formulaInputSource;
        private final ColumnSource<?> formulaOutputSource;

        private ByteChunk<? extends Values> influencerValuesChunk;
        private ByteRingBuffer byteWindowValues;

        @SuppressWarnings("unchecked")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize, influencerChunkSize);

            final String inputColumnName = pair.rightColumn;
            byteWindowValues = new ByteRingBuffer(BUFFER_INITIAL_CAPACITY, true);

            // Make a copy of the operator formula column.
            final FormulaColumn formulaCopy = (FormulaColumn)formulaColumn.copy();

            // Create a single value column source of the appropriate type for the formula column input.
            formulaInputSource = (SingleValueColumnSource<ByteVector>)SingleValueColumnSource.getSingleValueColumnSource(vectorType);
            formulaInputSource.set(new ByteRingBufferVectorWrapper(byteWindowValues));
            formulaCopy.initInputs(RowSetFactory.flat(1).toTracking(),
                    Collections.singletonMap(inputColumnName, formulaInputSource));

            formulaOutputSource = formulaCopy.getDataView();

        }

        @Override
        public void close() {
            super.close();
            byteWindowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asByteChunk();
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
                    writeNullToOutputChunk(ii);
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
        public void push(int pos, int count) {
            byteWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final byte val = influencerValuesChunk.get(pos + ii);
                byteWindowValues.addUnsafe(val);
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(byteWindowValues.size(), "byteWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                byteWindowValues.removeUnsafe();
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            // If not empty (even if completely full of null), run the formula over the window values.
            outputSetter.accept(formulaOutputSource.get(0), outIdx);
        }

        @Override
        public void reset() {
            byteWindowValues.clear();
        }
    }


    public ByteRollingFormulaOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String formula,
            @NotNull final String paramToken,
            @NotNull final ColumnSource<?> inputSource
            // region extra-constructor-args
                               ,final byte nullValue
            // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, formula, paramToken, inputSource);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }

    @Override
    public UpdateByOperator.@NotNull Context makeUpdateContext(int affectedChunkSize, int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}
