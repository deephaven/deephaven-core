//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharFillByOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseFloatUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatFillByOperator extends BaseFloatUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseFloatUpdateByOperator.Context {
        public FloatChunk<? extends Values> floatValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            floatValueChunk = valueChunks[0].asFloatChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            float val = floatValueChunk.get(pos);
            if(val != NULL_FLOAT) {
                curVal = val;
            }
        }
    }

    public FloatFillByOperator(
            @NotNull final MatchPair pair
            // region extra-constructor-args
            // endregion extra-constructor-args
            ) {
        super(pair, new String[] { pair.rightColumn });
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new FloatFillByOperator(
                pair
                // region extra-copy-args
                // endregion extra-copy-args
            );
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    // region extra-methods
    // endregion extra-methods
}
