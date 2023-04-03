package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.function.Basic;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

abstract class BasePrimitiveRollingWAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    final String weightColumnName;
    final ChunkType weightChunkType;
    final Class<?> weightColumnSourceType;

    // region extra-fields
    // endregion extra-fields

    abstract class Context extends BaseDoubleUpdateByOperator.Context {
        WritableDoubleChunk<? extends Values> influencerWeightValuesChunk;
        AggregatingDoubleRingBuffer windowValues;
        AggregatingDoubleRingBuffer windowWeightValues;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            windowValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return a;
                }
                return a + b;
            });
            windowWeightValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return a;
                }
                return a + b;
            });
            influencerWeightValuesChunk = WritableDoubleChunk.makeWritableChunk(influencerChunkSize);
        }

        @Override
        public void close() {
            super.close();
            windowValues = null;
            windowWeightValues = null;
            influencerWeightValuesChunk.close();
            influencerWeightValuesChunk = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            // Cast the weight values to double
            switch (weightChunkType) {
                case Char:
                    final CharChunk<?> charChunk = valueChunks[1].asCharChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final char val = charChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : (double) val);
                    }
                    break;
                case Byte:
                    final ByteChunk<?> byteChunk = valueChunks[1].asByteChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final byte val = byteChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : (double) val);
                    }
                    break;
                case Short:
                    final ShortChunk<?> shortChunk = valueChunks[1].asShortChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final short val = shortChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : (double) val);
                    }
                    break;
                case Long:
                    final LongChunk<?> longChunk = valueChunks[1].asLongChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final long val = longChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : (double) val);
                    }
                    break;
                case Float:
                    final FloatChunk<?> floatChunk = valueChunks[1].asFloatChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final float val = floatChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : (double) val);
                    }
                    break;
                case Double:
                    final DoubleChunk<?> doubleChunk = valueChunks[1].asDoubleChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final double val = doubleChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, val);
                    }
                    break;
                case Object:
                    if (weightColumnSourceType == BigInteger.class) {
                        final ObjectChunk<BigInteger, ?> objectChunk = valueChunks[1].asObjectChunk();
                        for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                            final BigInteger val = objectChunk.get(ii);
                            influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : val.doubleValue());
                        }
                    } else if (weightColumnSourceType == BigDecimal.class) {
                        final ObjectChunk<BigDecimal, ?> objectChunk = valueChunks[1].asObjectChunk();
                        for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                            final BigDecimal val = objectChunk.get(ii);
                            influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? NULL_DOUBLE : val.doubleValue());
                        }
                    } else {
                        throw new UnsupportedOperationException(
                                "RollingWAvgOperator weight column type not supported: " + weightColumnSourceType);
                    }
                    break;
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowValues.size(), "windowValues.size()", count);
            Assert.geq(windowWeightValues.size(), "windowWeightValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final double val = windowValues.removeUnsafe();
                windowWeightValues.removeUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (windowValues.size() == nullCount) {
                // Looks weird but returning NaN is consistent with Numeric#wavg and AggWAvg
                outputValues.set(outIdx, Double.NaN);
            } else {
                final double weightedValSum = windowValues.evaluate();
                final double weightSum = windowWeightValues.evaluate();

                // Divide by zero will result in NaN which is correct.
                outputValues.set(outIdx, weightedValSum / weightSum);
            }
        }

        @Override
        public void reset() {
            super.reset();
            windowValues.clear();
            windowWeightValues.clear();
        }
    }

    public BasePrimitiveRollingWAvgOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String weightColumnName,
            @NotNull final ChunkType weightChunkType,
            @NotNull final Class<?> weightColumnSourceType
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true);
        this.weightColumnName = weightColumnName;
        this.weightChunkType = weightChunkType;
        this.weightColumnSourceType = weightColumnSourceType;
        // region constructor
        // endregion constructor
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
