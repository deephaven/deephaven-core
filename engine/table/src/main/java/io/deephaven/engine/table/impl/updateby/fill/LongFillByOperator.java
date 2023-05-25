/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import java.time.Instant;
import java.util.Map;
import java.util.Collections;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongFillByOperator extends BaseLongUpdateByOperator {
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

            long val = longValueChunk.get(pos);
            if(val != NULL_LONG) {
                curVal = val;
            }
        }
    }

    public LongFillByOperator(@NotNull final MatchPair fillPair,
                              @Nullable final RowRedirection rowRedirection
                              // region extra-constructor-args
                              ,@NotNull final Class<?> type
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, rowRedirection);
        // region constructor
        this.type = type;
        // endregion constructor
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
