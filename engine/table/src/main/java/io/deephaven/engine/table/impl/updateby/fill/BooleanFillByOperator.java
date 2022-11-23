/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.engine.table.ColumnSource;
import java.util.Map;
import java.util.Collections;
import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.engine.table.impl.sources.BooleanSparseArraySource;
import io.deephaven.engine.table.WritableColumnSource;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;

public class BooleanFillByOperator extends BaseByteUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
        public ByteChunk<? extends Values> booleanValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            booleanValueChunk = valuesChunk.asByteChunk();
        }

        @Override
        public void push(long key, int pos) {
            byte currentVal = booleanValueChunk.get(pos);
            if(currentVal != NULL_BOOLEAN_AS_BYTE) {
                curVal = currentVal;
            }
        }
    }

    public BooleanFillByOperator(@NotNull final MatchPair fillPair,
                              @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper
                              // region extra-constructor-args
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, redirHelper);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
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
