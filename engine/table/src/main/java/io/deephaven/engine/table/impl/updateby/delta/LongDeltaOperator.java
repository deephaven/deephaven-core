//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharDeltaOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.delta;

import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.NullBehavior;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongDeltaOperator extends BaseLongUpdateByOperator {
    private final DeltaControl control;
    private ColumnSource<?> inputSource;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public LongChunk<? extends Values> longValueChunk;
        private long lastVal = NULL_LONG;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            longValueChunk = valueChunks[0].asLongChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final long currentVal = longValueChunk.get(pos);

            if (currentVal == NULL_LONG) {
                curVal = NULL_LONG;
            } else if (lastVal == NULL_LONG) {
                curVal = control.nullBehavior() == NullBehavior.NullDominates
                        ? NULL_LONG
                        : (control.nullBehavior() == NullBehavior.ZeroDominates
                                ? (long) 0
                                : currentVal);
            } else {
                curVal = (long) (currentVal - lastVal);
            }

            lastVal = currentVal;
        }
    }

    public LongDeltaOperator(
            @NotNull final MatchPair pair,
            @NotNull final DeltaControl control
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, new String[] {pair.rightColumn});
        this.control = control;
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new LongDeltaOperator(pair, control);
    }

    @Override
    public void initializeSources(@NotNull final Table source, @Nullable final RowRedirection rowRedirection) {
        super.initializeSources(source, rowRedirection);

        inputSource = ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(pair.rightColumn));
    }

    @Override
    public void initializeCumulative(
            @NotNull final UpdateByOperator.Context context,
            final long firstUnmodifiedKey,
            final long firstUnmodifiedTimestamp,
            @NotNull final RowSet bucketRowSet) {
        Context ctx = (Context) context;
        ctx.reset();
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            // Retrieve the value from the input column.
            ctx.lastVal = inputSource.getLong(firstUnmodifiedKey);
        } else {
            ctx.lastVal = NULL_LONG;
        }
    }

    // region extra-methods
    // endregion extra-methods

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}
