//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharFillByOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.engine.table.ColumnSource;
import java.util.Map;
import java.util.Collections;
import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.engine.table.impl.sources.BooleanSparseArraySource;
import io.deephaven.engine.table.WritableColumnSource;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;

public class BooleanFillByOperator extends BaseByteUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
        public ByteChunk<? extends Values> booleanValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            booleanValueChunk = valueChunks[0].asByteChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            byte val = booleanValueChunk.get(pos);
            if(val != NULL_BOOLEAN_AS_BYTE) {
                curVal = val;
            }
        }
    }

    public BooleanFillByOperator(
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
        return new BooleanFillByOperator(
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
    @Override
    protected byte getNullValue() {
        return NULL_BOOLEAN_AS_BYTE;
    }
    @Override
    protected WritableColumnSource<Byte> makeSparseSource() {
        return (WritableColumnSource<Byte>) new BooleanSparseArraySource().reinterpret(byte.class);
    }

    @Override
    protected WritableColumnSource<Byte> makeDenseSource() {
        return (WritableColumnSource<Byte>) new BooleanArraySource().reinterpret(byte.class);
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource.reinterpret(Boolean.class));
    }
    // endregion extra-methods
}
