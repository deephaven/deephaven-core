package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.cast.ToDoubleCast;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.deephaven.util.QueryConstants.*;

/**
 * This class will record the weight values and will accept primitive columns sources and store a window of values for
 * use by other operators.  It has no output columns but will be "affected" by all dependent columns
 */
public class DoubleRollingWAvgRecorder extends UpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    private final ChunkType srcChunkType;
    private final Set<String> affectingColumnNames;

    public class Context extends UpdateByOperator.Context {
        private final WritableDoubleChunk<Values> outputValues;
        private final ToDoubleCast castKernel;
        protected DoubleChunk<? extends Values> influencerValuesChunk;
        protected AggregatingDoubleRingBuffer windowValues;
        private double curVal;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            outputValues = WritableDoubleChunk.makeWritableChunk(affectedChunkSize);
            curVal = NULL_DOUBLE;
            castKernel = ToDoubleCast.makeToDoubleCast(srcChunkType, influencerChunkSize);
            windowValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return  a;
                }
                return a + b;
            });
        }

        @Override
        public void close() {
            windowValues = null;
            castKernel.close();
            outputValues.close();
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            // Convert the chunk of values to Long and store
            influencerValuesChunk = castKernel.cast(valuesChunk);
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final double val = influencerValuesChunk.get(pos + ii);

                if (val == NULL_DOUBLE) {
                    windowValues.addUnsafe(NULL_DOUBLE);
                    nullCount++;
                } else {
                    windowValues.addUnsafe(val);
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowValues.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final double weightedVal = windowValues.removeUnsafe();

                if (weightedVal == NULL_DOUBLE) {
                    nullCount--;
                }
            }
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
                    // Completely ignore this row.
                    continue;
                }

                // Pop for this row.
                if (popCount > 0) {
                    pop(popCount);
                }

                // Push for this row.
                if (pushCount > 0) {
                    push(pushIndex, pushCount);
                    pushIndex += pushCount;
                }

                // Write the results to the output chunk.
                writeToOutputChunk(ii);
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (windowValues.size() == nullCount) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                outputValues.set(outIdx, windowValues.evaluate());
            }
        }

        // region No-op functions

        @Override
        protected void writeToOutputColumn(@NotNull RowSequence inputKeys) {
        }

        @Override
        public void accumulateCumulative(RowSequence inputKeys, Chunk<? extends Values>[] valueChunkArr, LongChunk<? extends Values> tsChunk, int len) {
            throw Assert.statementNeverExecuted("DoubleRollingWAvgRecorder#accumulateCumulative()");
        }

        // endregion

        @Override
        public void reset() {
            windowValues.clear();
            curVal = NULL_DOUBLE;
        }
    }

    public DoubleRollingWAvgRecorder(@NotNull final String weightColumnName, @NotNull final ColumnSource<?> valueSource) {
        super(new MatchPair(weightColumnName, weightColumnName), null, null, null, 0, 0, true);
        srcChunkType = valueSource.getChunkType();
        affectingColumnNames = new HashSet<>();
        affectingColumnNames.add(weightColumnName);
    }

    /**
     * Add an affecting column name so this recorder is updated when the additional column is modified.  Should be called by dependent RollingWAvg operators on initialization.
     */
    public void addAffectingColumnName(String columnName) {
        affectingColumnNames.add(columnName);
    }

    /**
     * Get an array of column names that, when modified, affect the result of this computation.
     *
     * @return an array of column names that affect this operator.
     */
    @NotNull
    @Override
    protected String[] getAffectingColumnNames() {
        return affectingColumnNames.toArray(String[]::new);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public String getWeightColumn() {
        return pair.rightColumn;
    }

    public double getValue(final Context ctx, final int pos) {
        return ctx.influencerValuesChunk.get(pos);
    }

    public double getSum(final Context ctx, final int pos) {
        return ctx.outputValues.get(pos);
    }

    // region No-op functions
    @NotNull
    @Override
    protected Map<String, ColumnSource<?>> getOutputColumns() {
        return Map.of();
    }

    @Override
    protected void startTrackingPrev() {
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
    // endregion
}
