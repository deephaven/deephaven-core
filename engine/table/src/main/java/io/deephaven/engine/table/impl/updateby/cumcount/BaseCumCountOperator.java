//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.cumcount;

import io.deephaven.api.updateby.spec.CumCountSpec;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

public abstract class BaseCumCountOperator extends BaseLongUpdateByOperator {
    final CumCountSpec spec;
    final Class<?> columnType;

    /**
     * Functional interface for testing if the value at a given index in a {@link Chunk} is null.
     */
    @FunctionalInterface
    protected interface ValueCountFunction {
        boolean isCounted(int index);
    }

    protected class Context extends BaseLongUpdateByOperator.Context {
        private ValueCountFunction valueCountFunction;

        protected Context(final int chunkSize) {
            super(chunkSize);
            // Set to 0 as the initial value (vs. default of NULL_LONG)
            curVal = 0;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            valueCountFunction = createValueCountFunction(valueChunks[0], spec.countType());
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

    /**
     * Create a function that tests a {@link Chunk chunk} value at a given index.
     *
     * @param chunk the generic {@link Chunk} to test values
     * @return the test function appropriate for the given chunk type
     */
    abstract ValueCountFunction createValueCountFunction(
            final Chunk<? extends Values> chunk,
            final CumCountSpec.CumCountType countType);

    public BaseCumCountOperator(
            @NotNull final MatchPair pair,
            @NotNull final CumCountSpec spec,
            @NotNull final Class<?> columnType) {
        super(pair, new String[] {pair.rightColumn});
        this.spec = spec;
        this.columnType = columnType;
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
