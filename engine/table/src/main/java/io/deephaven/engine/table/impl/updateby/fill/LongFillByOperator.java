/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.engine.table.ColumnSource;
import java.util.Map;
import java.util.Collections;
import io.deephaven.time.DateTime;
import java.time.Instant;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongFillByOperator extends BaseLongUpdateByOperator {
    // region extra-fields
    private final Class<?> type;
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public LongChunk<? extends Values> longValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            longValueChunk = valuesChunk.asLongChunk();
        }

        @Override
        public void push(long key, int pos) {
            long currentVal = longValueChunk.get(pos);
            if(currentVal != NULL_LONG) {
                curVal = currentVal;
            }
        }

        @Override
        public void reset() {
            curVal = NULL_LONG;
        }
    }

    public LongFillByOperator(@NotNull final MatchPair fillPair,
                              @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                              // region extra-constructor-args
                              ,@NotNull final Class<?> type
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, redirContext);
        // region constructor
        this.type = type;
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    // region extra-methods
    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        final ColumnSource<?> actualOutput;
        if(type == DateTime.class) {
            actualOutput = ReinterpretUtils.longToDateTimeSource(outputSource);
        } else {
            actualOutput = outputSource;
        }
        return Collections.singletonMap(pair.leftColumn, actualOutput);
    }
    // endregion extra-methods
}
