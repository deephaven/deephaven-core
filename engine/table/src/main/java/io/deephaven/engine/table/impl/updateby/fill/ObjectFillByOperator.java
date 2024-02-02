/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.engine.table.impl.util.ChunkUtils;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;


public class ObjectFillByOperator<T> extends BaseObjectUpdateByOperator<T> {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        public ObjectChunk<T, ? extends Values> ObjectValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            ObjectValueChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            T val = ObjectValueChunk.get(pos);
            if(val != null) {
                curVal = val;
            }
        }
    }

    public ObjectFillByOperator(
            @NotNull final MatchPair pair
            // region extra-constructor-args
            , final Class<T> colType
            // endregion extra-constructor-args
            ) {
        super(pair, new String[] { pair.rightColumn }, colType);
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new ObjectFillByOperator(
                pair
                // region extra-copy-args
                , colType
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
