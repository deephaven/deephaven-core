/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.engine.table.ColumnSource;
import java.util.Map;
import java.util.Collections;
import io.deephaven.time.DateTime;
import java.time.Instant;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class LongCumMinMaxOperator extends BaseLongUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    private final Class<?> type;
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public LongChunk<Values> longValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            longValueChunk = valuesChunk.asLongChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final long currentVal = longValueChunk.get(pos);

            if (curVal == NULL_LONG) {
                curVal = currentVal;
            } else if (currentVal != NULL_LONG) {
                if ((isMax && currentVal > curVal) ||
                        (!isMax && currentVal < curVal)) {
                    curVal = currentVal;
                }
            }
        }

        @Override
        public void reset() {
            curVal = NULL_LONG;
        }
    }

    public LongCumMinMaxOperator(@NotNull final MatchPair pair,
                                  final boolean isMax,
                                  @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                // region extra-constructor-args
                              ,@NotNull final Class<?> type
                                // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, redirContext);
        this.isMax = isMax;
        // region constructor
        this.type = type;
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }
}
