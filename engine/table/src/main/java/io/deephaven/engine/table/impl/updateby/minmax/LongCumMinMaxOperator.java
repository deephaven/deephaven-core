//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ShortCumMinMaxOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.minmax;

import java.time.Instant;
import java.util.Map;
import java.util.Collections;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class LongCumMinMaxOperator extends BaseLongUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    private final Class<?> type;
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public LongChunk<? extends Values> longValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            longValueChunk = valueChunks[0].asLongChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            final long val = longValueChunk.get(pos);

            if (curVal == NULL_LONG) {
                curVal = val;
            } else if (val != NULL_LONG) {
                if ((isMax && val > curVal) ||
                        (!isMax && val < curVal)) {
                    curVal = val;
                }
            }
        }
    }

    public LongCumMinMaxOperator(
            @NotNull final MatchPair pair,
            final boolean isMax
    // region extra-constructor-args
            ,@NotNull final Class<?> type
    // endregion extra-constructor-args
    ) {
        super(pair, new String[] {pair.rightColumn});
        this.isMax = isMax;
        // region constructor
        this.type = type;
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new LongCumMinMaxOperator(
                pair,
                isMax
        // region extra-copy-args
                , type
        // endregion extra-copy-args
        );
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    // region extra-methods
    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        final ColumnSource<?> actualOutput;
        if(type == Instant.class) {
            actualOutput = ReinterpretUtils.longToInstantSource(outputSource);
        } else {
            actualOutput = outputSource;
        }
        return Collections.singletonMap(pair.leftColumn, actualOutput);
    }
    // endregion extra-methods
}
