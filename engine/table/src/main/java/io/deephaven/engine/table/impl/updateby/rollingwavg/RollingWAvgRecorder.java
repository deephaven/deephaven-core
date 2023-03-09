package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Map;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * This class will record the weight values and will accept primitive, floating point or BigInteger/BigDecimal columns
 * sources and store a window of values for use by other operators.  It has no output columns but will be "affected" by
 * all dependent columns
 */
public class RollingWAvgRecorder extends UpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;

    public enum ColumnType {
        PRIMITIVE,
        FLOATING,
        BIGNUMBER;
    }

    private final ColumnType columnType;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateByOperator.Context {
        /**
         * When the source is a primitive, we will store the weight values in these members.
         */
        protected LongRingBuffer primitiveWindowValues;
        private long curPrimitiveVal;

        /**
         * When the source is a floating point, we will store the weight values in these members.
         */
        protected AggregatingDoubleRingBuffer floatWindowValues;
        private double curFloatVal;

        /**
         * When the source is a BigInteger/BigDecimal, we will store the weight values in these members.
         */
        protected ObjectRingBuffer<BigDecimal> bigNumberWindowValues;
        private BigDecimal curBigNumberVal;

        protected Context() {
            if (columnType == ColumnType.FLOATING) {
                floatWindowValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0f, (a, b) -> {
                    if (a == NULL_DOUBLE) {
                        return b;
                    } else if (b == NULL_DOUBLE) {
                        return  a;
                    }
                    return a + b;
                });
                curFloatVal = NULL_DOUBLE;
            } else if (columnType == ColumnType.PRIMITIVE) {
                primitiveWindowValues = new LongRingBuffer(BUFFER_INITIAL_CAPACITY, true);
                curPrimitiveVal = NULL_LONG;
            } else {
                bigNumberWindowValues = new ObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, true);
                curBigNumberVal = null;
            }
        }

        @Override
        public void close() {
            primitiveWindowValues = null;
            floatWindowValues = null;
            if (bigNumberWindowValues != null) {
                // Call clear() to reset the objects to null
                bigNumberWindowValues.clear();
            }
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
        }

        @Override
        public void push(int pos, int count) {
        }

        @Override
        public void pop(int count) {
        }

        @Override
        public void accumulateCumulative(RowSequence inputKeys, Chunk<? extends Values>[] valueChunkArr, LongChunk<? extends Values> tsChunk, int len) {
        }

        @Override
        public void accumulateRolling(RowSequence inputKeys,
                                      Chunk<? extends Values>[] influencerValueChunkArr,
                                      LongChunk<OrderedRowKeys> affectedPosChunk,
                                      LongChunk<OrderedRowKeys> influencerPosChunk,
                                      IntChunk<? extends Values> pushChunk,
                                      IntChunk<? extends Values> popChunk,
                                      int len) {
            setValuesChunk(influencerValueChunkArr[0]);
            setPosChunks(affectedPosChunk, influencerPosChunk);

            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                final int pushCount = pushChunk.get(ii);
                final int popCount = popChunk.get(ii);

                if (pushCount == NULL_INT) {
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

                // compute the results (but store instead of writing)
                writeToOutputChunk(ii);
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
        }

        @Override
        protected void writeToOutputColumn(@NotNull RowSequence inputKeys) {
        }

        @Override
        public void reset() {
            if (floatWindowValues != null) {
                floatWindowValues.clear();
                curFloatVal = NULL_DOUBLE;
                return;
            }
            if (primitiveWindowValues != null) {
                primitiveWindowValues.clear();
                curPrimitiveVal = NULL_LONG;
                return;
            }
            bigNumberWindowValues.clear();
            curBigNumberVal = null;
        }
    }

    @NotNull
    @Override
    protected Map<String, ColumnSource<?>> getOutputColumns() {
        return Map.of();
    }

    @Override
    protected void startTrackingPrev() {
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context();
    }

    @Override
    protected void applyOutputShift(@NotNull RowSet subRowSetToShift, long delta) {
    }

    @Override
    protected void prepareForParallelPopulation(RowSet changedRows) {
    }

    @Override
    protected void clearOutputRows(RowSet toClear) {
    }

    public RollingWAvgRecorder(@NotNull final MatchPair pair,
                               @NotNull final String[] affectingColumns,
                               @Nullable final RowRedirection rowRedirection,
                               @Nullable final String timestampColumnName,
                               final long reverseWindowScaleUnits,
                               final long forwardWindowScaleUnits
                               // region extra-constructor-args
                               // endregion extra-constructor-args
    ) {
        super(null, null, null, null, 0, 0, true);
        columnType = ColumnType.FLOATING;
        // region constructor
        // endregion constructor
    }
}
