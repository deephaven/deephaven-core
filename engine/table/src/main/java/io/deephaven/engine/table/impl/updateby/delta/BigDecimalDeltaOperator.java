//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.delta;

import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.NullBehavior;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public class BigDecimalDeltaOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    private final DeltaControl control;
    private ColumnSource<?> inputSource;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<BigDecimal, ? extends Values> objectValueChunk;
        private BigDecimal lastVal = null;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectValueChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final BigDecimal currentVal = objectValueChunk.get(pos);

            if (currentVal == null) {
                curVal = null;
            } else if (lastVal == null) {
                curVal = control.nullBehavior() == NullBehavior.NullDominates
                        ? null
                        : (control.nullBehavior() == NullBehavior.ZeroDominates
                                ? BigDecimal.ZERO
                                : currentVal);
            } else {
                curVal = currentVal.subtract(lastVal);
            }

            lastVal = currentVal;
        }
    }

    public BigDecimalDeltaOperator(@NotNull final MatchPair pair, @NotNull final DeltaControl control) {
        super(pair, new String[] {pair.rightColumn}, BigDecimal.class);
        this.control = control;
    }

    @Override
    public UpdateByOperator copy() {
        return new BigDecimalDeltaOperator(pair, control);
    }

    @Override
    public void initializeSources(@NotNull final Table source, @Nullable final RowRedirection rowRedirection) {
        super.initializeSources(source, rowRedirection);

        inputSource = source.getColumnSource(pair.rightColumn);
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
            ctx.lastVal = (BigDecimal) inputSource.get(firstUnmodifiedKey);
        } else {
            ctx.lastVal = null;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}
