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
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public class BigIntegerDeltaOperator extends BaseObjectUpdateByOperator<BigInteger> {
    private final DeltaControl control;
    private ColumnSource<?> inputSource;

    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        public ObjectChunk<BigInteger, ? extends Values> objectValueChunk;
        private BigInteger lastVal = null;

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
            final BigInteger currentVal = objectValueChunk.get(pos);

            if (currentVal == null) {
                curVal = null;
            } else if (lastVal == null) {
                curVal = control.nullBehavior() == NullBehavior.NullDominates
                        ? null
                        : (control.nullBehavior() == NullBehavior.ZeroDominates
                                ? BigInteger.ZERO
                                : currentVal);
            } else {
                curVal = currentVal.subtract(lastVal);
            }

            lastVal = currentVal;
        }
    }

    public BigIntegerDeltaOperator(@NotNull final MatchPair pair, @NotNull final DeltaControl control) {
        super(pair, new String[] {pair.rightColumn}, BigInteger.class);
        this.control = control;
    }

    @Override
    public UpdateByOperator copy() {
        return new BigIntegerDeltaOperator(pair, control);
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
            ctx.lastVal = (BigInteger) inputSource.get(firstUnmodifiedKey);
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
