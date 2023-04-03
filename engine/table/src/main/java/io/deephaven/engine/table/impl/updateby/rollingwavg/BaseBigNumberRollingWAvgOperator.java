package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.function.Basic;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public abstract class BaseBigNumberRollingWAvgOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    @NotNull
    final MathContext mathContext;
    final String weightColumnName;
    final ChunkType weightChunkType;
    final Class<?> weightColumnSourceType;

    protected abstract class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<T, ? extends Values> influencerValuesChunk;
        protected WritableObjectChunk<BigDecimal, ? extends Values> influencerWeightValuesChunk;
        protected AggregatingObjectRingBuffer<BigDecimal> windowValues;
        protected AggregatingObjectRingBuffer<BigDecimal> windowWeightValues;

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
            influencerWeightValuesChunk = WritableObjectChunk.makeWritableChunk(influencerChunkSize);
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
            influencerValuesChunk = valueChunks[0].asObjectChunk();

            // Upcast the weight values to BigDecimal
            switch (weightChunkType) {
                case Char:
                    final CharChunk<?> charChunk = valueChunks[1].asCharChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final char val = charChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : BigDecimal.valueOf(val));
                    }
                    break;
                case Byte:
                    final ByteChunk<?> byteChunk = valueChunks[1].asByteChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final byte val = byteChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : BigDecimal.valueOf(val));
                    }
                    break;
                case Short:
                    final ShortChunk<?> shortChunk = valueChunks[1].asShortChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final short val = shortChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : BigDecimal.valueOf(val));
                    }
                    break;
                case Long:
                    final LongChunk<?> longChunk = valueChunks[1].asLongChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final long val = longChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : BigDecimal.valueOf(val));
                    }
                    break;
                case Float:
                    final FloatChunk<?> floatChunk = valueChunks[1].asFloatChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final float val = floatChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : BigDecimal.valueOf(val));
                    }
                    break;
                case Double:
                    final DoubleChunk<?> doubleChunk = valueChunks[1].asDoubleChunk();
                    for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                        final double val = doubleChunk.get(ii);
                        influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : BigDecimal.valueOf(val));
                    }
                    break;
                case Object:
                    if (weightColumnSourceType == BigInteger.class) {
                        final ObjectChunk<BigInteger, ?> objectChunk = valueChunks[1].asObjectChunk();
                        for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                            final BigInteger val = objectChunk.get(ii);
                            influencerWeightValuesChunk.set(ii, Basic.isNull(val) ? null : new BigDecimal(val));
                        }
                    } else if (weightColumnSourceType == BigDecimal.class) {
                        final ObjectChunk<BigDecimal, ?> objectChunk = valueChunks[1].asObjectChunk();
                        for (int ii = 0; ii < valueChunks[1].size(); ii++) {
                            final BigDecimal val = objectChunk.get(ii);
                            influencerWeightValuesChunk.set(ii, val);
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
                final BigDecimal val = windowValues.removeUnsafe();
                windowWeightValues.removeUnsafe();

                if (val == null) {
                    nullCount--;
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

    public BaseBigNumberRollingWAvgOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String weightColumnName,
            @NotNull final ChunkType weightChunkType,
            @NotNull final Class<?> weightColumnSourceType,
            @NotNull final MathContext mathContext
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true, BigDecimal.class);
        this.weightColumnName = weightColumnName;
        this.weightChunkType = weightChunkType;
        this.weightColumnSourceType = weightColumnSourceType;
        this.mathContext = mathContext;
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
