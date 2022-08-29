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

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongCumMinMaxOperator extends BaseLongUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    private final Class<?> type;
    // endregion extra-fields

    public LongCumMinMaxOperator(@NotNull final MatchPair inputPair,
                                  final boolean isMax,
                                  @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                  // region extra-constructor-args
                              ,@NotNull final Class<?> type
                                  // endregion extra-constructor-args
                                  ) {
        super(inputPair, new String[] { inputPair.rightColumn }, redirContext);
        this.isMax = isMax;
        // region constructorÃ
        this.type = type;
        // endregion constructor
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

    @Override
    protected void doProcessChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        accumulate(workingChunk.asLongChunk(), ctx, 0, workingChunk.size());
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final LongChunk<Values> asLongs,
                            @NotNull final Context ctx,
                            final int runStart,
                            final int runLength) {
        final WritableLongChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final long currentVal = asLongs.get(ii);
            if(ctx.curVal == NULL_LONG) {
                ctx.curVal = currentVal;
            } else if(currentVal != NULL_LONG) {
                if((isMax && currentVal > ctx.curVal) ||
                   (!isMax && currentVal < ctx.curVal)  ) {
                    ctx.curVal = currentVal;
                }
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
