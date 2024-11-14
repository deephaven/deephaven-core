//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.cumcount;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

/**
 * Incredibly simple CumCount operator that accumulates a count of all rows.
 */
public class CumCountAllOperator extends BaseLongUpdateByOperator {
    protected class Context extends BaseLongUpdateByOperator.Context {
        protected Context(final int chunkSize) {
            super(chunkSize);
            // Set to 0 as the initial value (vs. default of NULL_LONG)
            curVal = 0;
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);
            curVal++;
        }

        @Override
        public void reset() {
            super.reset();
            // Reset the current value to 0
            curVal = 0;
        }
    }

    public CumCountAllOperator(
            @NotNull final MatchPair pair) {
        super(pair, new String[] {pair.rightColumn});
    }

    @Override
    public UpdateByOperator copy() {
        return new CumCountAllOperator(pair);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
