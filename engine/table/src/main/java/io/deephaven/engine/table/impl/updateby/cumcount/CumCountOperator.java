//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.cumcount;

import io.deephaven.api.agg.util.AggCountType;
import io.deephaven.api.updateby.spec.CumCountSpec;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

public class CumCountOperator extends BaseLongUpdateByOperator {
    final CumCountSpec spec;
    final Class<?> columnType;

    /**
     * Functional interface for testing if the value at a given index in a {@link Chunk} should be included in the
     * count.
     */
    @FunctionalInterface
    protected interface ValueCountFunction {
        boolean isCounted(int index);
    }

    protected class Context extends BaseLongUpdateByOperator.Context {
        private ValueCountFunction valueCountFunction;
        private Chunk<? extends Values> chunkReference;

        protected Context(final int chunkSize) {
            super(chunkSize);
            // Set to 0 as the initial value (vs. default of NULL_LONG)
            curVal = 0;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            // Update the value count function initially and when the chunk reference changes.
            if (!Objects.equals(valueChunks[0], chunkReference)) {
                valueCountFunction = createValueCountFunction(valueChunks[0], spec.countType());
                chunkReference = valueChunks[0];
            }
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // Increment the current value when the test passes
            if (valueCountFunction.isCounted(pos)) {
                curVal++;
            }
        }

        @Override
        public void reset() {
            super.reset();
            // Reset the current value to 0
            curVal = 0;
        }
    }

    public CumCountOperator(
            @NotNull final MatchPair pair,
            @NotNull final CumCountSpec spec,
            @NotNull final Class<?> columnType) {
        super(pair, new String[] {pair.rightColumn});
        this.spec = spec;
        this.columnType = columnType;
    }

    @Override
    public UpdateByOperator copy() {
        return new CumCountOperator(pair, spec, columnType);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    /**
     * Create a function that tests a {@link Chunk chunk} value at a given index for the provided {@link AggCountType}
     * and the column type of this operator.
     *
     * @param chunk the generic {@link Chunk} to test values
     * @return the test function appropriate for the given chunk type
     */
    protected ValueCountFunction createValueCountFunction(
            final Chunk<? extends Values> chunk,
            final AggCountType countType) {
        if (columnType == byte.class || columnType == Byte.class) {
            return createByteValueCountFunction(chunk.asByteChunk(), countType);
        }
        if (columnType == char.class || columnType == Character.class) {
            return createCharValueCountFunction(chunk.asCharChunk(), countType);
        }
        if (columnType == double.class || columnType == Double.class) {
            return createDoubleValueCountFunction(chunk.asDoubleChunk(), countType);
        }
        if (columnType == float.class || columnType == Float.class) {
            return createFloatValueCountFunction(chunk.asFloatChunk(), countType);
        }
        if (columnType == int.class || columnType == Integer.class) {
            return createIntValueCountFunction(chunk.asIntChunk(), countType);
        }
        if (columnType == long.class || columnType == Long.class) {
            return createLongValueCountFunction(chunk.asLongChunk(), countType);
        }
        if (columnType == short.class || columnType == Short.class) {
            return createShortValueCountFunction(chunk.asShortChunk(), countType);
        }
        if (columnType == BigDecimal.class) {
            return createBigDecimalValueCountFunction(chunk, countType);
        }
        if (columnType == BigInteger.class) {
            return createBigIntegerValueCountFunction(chunk, countType);
        }
        if (columnType == boolean.class || columnType == Boolean.class) {
            return createBooleanValueCountFunction(chunk, countType);
        }
        return createObjectValueCountFunction(chunk, countType);
    }

    // region ValueCountFunction implementations

    private ValueCountFunction createByteValueCountFunction(
            @NotNull final ByteChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.ByteCountFunction byteCountFunction = AggCountType.getByteCountFunction(countType);
        return index -> byteCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createCharValueCountFunction(
            @NotNull final CharChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.CharCountFunction charCountFunction = AggCountType.getCharCountFunction(countType);
        return index -> charCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createDoubleValueCountFunction(
            @NotNull final DoubleChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.DoubleCountFunction doubleCountFunction = AggCountType.getDoubleCountFunction(countType);
        return index -> doubleCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createFloatValueCountFunction(
            @NotNull final FloatChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.FloatCountFunction floatCountFunction = AggCountType.getFloatCountFunction(countType);
        return index -> floatCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createIntValueCountFunction(
            @NotNull final IntChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.IntCountFunction intCountFunction = AggCountType.getIntCountFunction(countType);
        return index -> intCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createLongValueCountFunction(
            @NotNull final LongChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.LongCountFunction longCountFunction = AggCountType.getLongCountFunction(countType);
        return index -> longCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createShortValueCountFunction(
            @NotNull final ShortChunk<?> valueChunk,
            final AggCountType countType) {
        final AggCountType.ShortCountFunction shortCountFunction = AggCountType.getShortCountFunction(countType);
        return index -> shortCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createBooleanValueCountFunction(
            final Chunk<? extends Values> chunk,
            final AggCountType countType) {
        final ByteChunk<?> valueChunk = chunk.asByteChunk();
        final AggCountType.BooleanCountFunction booleanCountFunction = AggCountType.getBooleanCountFunction(countType);
        return index -> booleanCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createBigDecimalValueCountFunction(
            final Chunk<? extends Values> chunk,
            final AggCountType countType) {
        final ObjectChunk<BigDecimal, ? extends Values> valueChunk = chunk.asObjectChunk();
        final AggCountType.BigDecimalCountFunction bdCountFunction = AggCountType.getBigDecimalCountFunction(countType);
        return index -> bdCountFunction.count(valueChunk.get(index));
    }

    private ValueCountFunction createBigIntegerValueCountFunction(
            final Chunk<? extends Values> chunk,
            final AggCountType countType) {
        final ObjectChunk<BigInteger, ? extends Values> valueChunk = chunk.asObjectChunk();
        final AggCountType.BigIntegerCountFunction biCountFunction = AggCountType.getBigIntegerCountFunction(countType);
        return index -> biCountFunction.count(valueChunk.get(index));
    }

    protected ValueCountFunction createObjectValueCountFunction(
            final Chunk<? extends Values> chunk,
            final AggCountType countType) {
        final ObjectChunk<Object, ? extends Values> valueChunk = chunk.asObjectChunk();
        final AggCountType.ObjectCountFunction objectCountFunction = AggCountType.getObjectCountFunction(countType);
        return index -> objectCountFunction.count(valueChunk.get(index));
    }
    // endregion

}
