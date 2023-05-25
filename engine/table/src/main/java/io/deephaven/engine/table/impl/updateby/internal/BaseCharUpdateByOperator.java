package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

public abstract class BaseCharUpdateByOperator extends UpdateByOperator {
    protected final WritableColumnSource<Character> outputSource;
    protected final WritableColumnSource<Character> maybeInnerSource;

    // region extra-fields
    // endregion extra-fields

    protected abstract class Context extends UpdateByOperator.Context {
        protected final ChunkSink.FillFromContext outputFillContext;
        protected final WritableCharChunk<Values> outputValues;

        public char curVal = NULL_CHAR;

        protected Context(final int chunkSize) {
            this.outputFillContext = outputSource.makeFillFromContext(chunkSize);
            this.outputValues = WritableCharChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public void accumulateCumulative(@NotNull final RowSequence inputKeys,
                                         @NotNull final Chunk<? extends Values>[] valueChunkArr,
                                         @Nullable final LongChunk<? extends Values> tsChunk,
                                         final int len) {

            setValueChunks(valueChunkArr);

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                push(ii, 1);
                writeToOutputChunk(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void accumulateRolling(@NotNull final RowSequence inputKeys,
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
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {}

        @Override
        public void writeToOutputChunk(final int outIdx) {
            outputValues.set(outIdx, curVal);
        }

        void writeNullToOutputChunk(final int outIdx) {
            outputValues.set(outIdx, NULL_CHAR);
        }

        @Override
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
        }

        @Override
        public void reset() {
            curVal = NULL_CHAR;
            nullCount = 0;
        }

        @Override
        public void close() {
            outputValues.close();
            outputFillContext.close();
        }
    }

    /**
     * Construct a base operator for operations that produce char outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param rowRedirection the {@link RowRedirection} for the output column
     */
    public BaseCharUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
    ) {
        this(pair, affectingColumns, rowRedirection, null, 0, 0, false);
    }

    /**
     * Construct a base operator for operations that produce char outputs.
     *
     * @param pair             the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns a list of all columns (including the input column from the pair) that affects the result
     *                         of this operator.
     * @param rowRedirection the {@link RowRedirection} for the output column
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param reverseWindowScaleUnits the reverse window for the operator. If no {@code timestampColumnName} is provided, this
     *                       is measured in ticks, otherwise it is measured in nanoseconds.
     * @param forwardWindowScaleUnits the forward window for the operator. If no {@code timestampColumnName} is provided, this
     *                       is measured in ticks, otherwise it is measured in nanoseconds.
     */
    public BaseCharUpdateByOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection,
                                    @Nullable final String timestampColumnName,
                                    final long reverseWindowScaleUnits,
                                    final long forwardWindowScaleUnits,
                                    final boolean isWindowed
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, isWindowed);
        if(rowRedirection != null) {
            // region create-dense
            this.maybeInnerSource = new CharacterArraySource();
            // endregion create-dense
            this.outputSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, maybeInnerSource, 0);
        } else {
            this.maybeInnerSource = null;
            // region create-sparse
            this.outputSource = new CharacterSparseArraySource();
            // endregion create-sparse
        }

        // region constructor
        // endregion constructor
    }


    // region extra-methods
    // endregion extra-methods

    @Override
    public void initializeCumulative(@NotNull final UpdateByOperator.Context context,
                                     final long firstUnmodifiedKey,
                                     final long firstUnmodifiedTimestamp,
                                     @NotNull final RowSet bucketRowSet) {
        Context ctx = (Context) context;
        ctx.reset();
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            ctx.curVal = outputSource.getChar(firstUnmodifiedKey);
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

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((CharacterSparseArraySource)outputSource).shift(subRowSetToShift, delta);
    }
    // endregion Shifts

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        if (rowRedirection != null) {
            assert maybeInnerSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) maybeInnerSource).prepareForParallelPopulation(changedRows);
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
        // NOP for primitive types
    }
    // endregion clear-output
}
