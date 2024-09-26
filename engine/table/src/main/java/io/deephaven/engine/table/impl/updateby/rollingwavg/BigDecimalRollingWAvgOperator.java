//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.cast.ToBigDecimalCast;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

public class BigDecimalRollingWAvgOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    @NotNull
    final MathContext mathContext;
    @NotNull
    final String weightColumnName;
    @NotNull
    protected ColumnSource<?> weightColumnSource;
    @NotNull
    protected ColumnSource<?> valueColumnSource;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        ObjectChunk<BigDecimal, ? extends Any> influencerValuesChunk;
        ObjectChunk<BigDecimal, ? extends Any> influencerWeightValuesChunk;
        AggregatingObjectRingBuffer<BigDecimal> windowValues;
        AggregatingObjectRingBuffer<BigDecimal> windowWeightValues;

        ToBigDecimalCast valueCast;
        ToBigDecimalCast weightCast;


        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            windowValues = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, BigDecimal.ZERO,
                    BigDecimal::add,
                    (a, b) -> {
                        if (a == null && b == null) {
                            // Return the identity value.
                            return BigDecimal.ZERO;
                        } else if (a == null) {
                            return b;
                        } else if (b == null) {
                            return a;
                        }
                        return a.add(b);
                    });
            windowWeightValues = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, BigDecimal.ZERO,
                    BigDecimal::add,
                    (a, b) -> {
                        if (a == null && b == null) {
                            // Return the identity value.
                            return BigDecimal.ZERO;
                        } else if (a == null) {
                            return b;
                        } else if (b == null) {
                            return a;
                        }
                        return a.add(b);
                    });
            // Create casting helpers for the weight and value sources.
            weightCast = ToBigDecimalCast.makeToBigDecimalCast(weightColumnSource.getChunkType(),
                    weightColumnSource.getType(), influencerChunkSize);
            valueCast = ToBigDecimalCast.makeToBigDecimalCast(valueColumnSource.getChunkType(),
                    valueColumnSource.getType(), influencerChunkSize);
        }

        @Override
        public void close() {
            super.close();
            windowValues = null;
            windowWeightValues = null;
            valueCast.close();
            weightCast.close();
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueCast.cast(valueChunks[0]);
            influencerWeightValuesChunk = weightCast.cast(valueChunks[1]);
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowValues.size(), "windowValues.size()", count);
            Assert.geq(windowWeightValues.size(), "windowWeightValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final BigDecimal val = windowValues.removeUnsafe();
                windowWeightValues.removeUnsafe();

                if (val == null) {
                    nullCount--;
                }
            }
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);
            windowWeightValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final BigDecimal val = influencerValuesChunk.get(pos + ii);
                final BigDecimal weight = influencerWeightValuesChunk.get(pos + ii);

                if (val == null || weight == null) {
                    windowValues.addUnsafe(null);
                    windowWeightValues.addUnsafe(null);
                    nullCount++;
                } else {
                    // Compute the product and add to the agg buffer.
                    final BigDecimal weightedVal = weight.multiply(val, mathContext);
                    windowValues.addUnsafe(weightedVal);
                    windowWeightValues.addUnsafe(weight);
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (windowValues.size() == nullCount) {
                // This is inconsistent with the primitives, but BigDecimal cannot represent NaN.
                outputValues.set(outIdx, null);
            } else {
                final BigDecimal weightSum = windowWeightValues.evaluate();
                // Divide by zero will result in null.
                if (!weightSum.equals(BigDecimal.ZERO)) {
                    final BigDecimal weightedValSum = windowValues.evaluate();
                    outputValues.set(outIdx, weightedValSum.divide(weightSum, mathContext));
                } else {
                    outputValues.set(outIdx, null);
                }
            }
        }

        @Override
        public void reset() {
            super.reset();
            windowValues.clear();
            windowWeightValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public BigDecimalRollingWAvgOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String weightColumnName,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true, BigDecimal.class);
        this.weightColumnName = weightColumnName;
        this.mathContext = mathContext;
    }

    @Override
    public void initializeSources(@NotNull final Table source, @Nullable final RowRedirection rowRedirection) {
        super.initializeSources(source, rowRedirection);

        valueColumnSource = ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(pair.rightColumn));
        weightColumnSource = ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(weightColumnName));
    }

    @Override
    public UpdateByOperator copy() {
        return new BigDecimalRollingWAvgOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                weightColumnName,
                mathContext);
    }

    /**
     * Get the names of the input column(s) for this operator.
     *
     * @return the names of the input column
     */
    @NotNull
    @Override
    protected String[] getInputColumnNames() {
        return new String[] {pair.rightColumn, weightColumnName};
    }
}
