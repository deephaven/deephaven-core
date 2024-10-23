//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.cumcount;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class CumCountOperator extends BaseLongUpdateByOperator {

    /**
     * Functional interface for testing if the value at a given index in a {@link Chunk} is null.
     */
    @FunctionalInterface
    private interface ValueNullFunction {
        boolean isNull(int index);
    }

    protected class Context extends BaseLongUpdateByOperator.Context {
        private ValueNullFunction valueNullFunction;

        protected Context(final int chunkSize) {
            super(chunkSize);
            // Set to 0 as the initial value (vs. default of NULL_LONG)
            curVal = 0;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            valueNullFunction = createValueNullFunction(valueChunks[0]);
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            if (!valueNullFunction.isNull(pos)) {
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

    /**
     * Create a function that tests {@link Chunk chunk} for a null value at a given index.
     *
     * @param chunk the generic {@link Chunk} to test values
     * @return the test function appropriate for the given chunk type
     */
    private ValueNullFunction createValueNullFunction(final Chunk<? extends Values> chunk) {
        if (chunk instanceof ByteChunk) {
            final ByteChunk<?> byteChunk = (ByteChunk<?>) chunk;
            return index -> byteChunk.get(index) == NULL_BYTE;
        }
        if (chunk instanceof CharChunk) {
            final CharChunk<?> charChunk = (CharChunk<?>) chunk;
            return index -> charChunk.get(index) == NULL_CHAR;
        }
        if (chunk instanceof DoubleChunk) {
            final DoubleChunk<?> doubleChunk = (DoubleChunk<?>) chunk;
            return index -> doubleChunk.get(index) == NULL_DOUBLE;
        }
        if (chunk instanceof FloatChunk) {
            final FloatChunk<?> floatChunk = (FloatChunk<?>) chunk;
            return index -> floatChunk.get(index) == NULL_FLOAT;
        }
        if (chunk instanceof IntChunk) {
            final IntChunk<?> intChunk = (IntChunk<?>) chunk;
            return index -> intChunk.get(index) == NULL_INT;
        }
        if (chunk instanceof LongChunk) {
            final LongChunk<?> longChunk = (LongChunk<?>) chunk;
            return index -> longChunk.get(index) == NULL_LONG;
        }
        if (chunk instanceof ShortChunk) {
            final ShortChunk<?> shortChunk = (ShortChunk<?>) chunk;
            return index -> shortChunk.get(index) == NULL_SHORT;
        }
        final ObjectChunk<?, ?> objectChunk = (ObjectChunk<?, ?>) chunk;
        return index -> objectChunk.get(index) == null;
    }

    public CumCountOperator(@NotNull final MatchPair pair) {
        super(pair, new String[] {pair.rightColumn});
    }

    @Override
    public UpdateByOperator copy() {
        return new CumCountOperator(pair);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
