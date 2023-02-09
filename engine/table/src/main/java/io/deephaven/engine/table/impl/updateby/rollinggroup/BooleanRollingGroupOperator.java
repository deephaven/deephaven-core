package io.deephaven.engine.table.impl.updateby.rollinggroup;

import io.deephaven.engine.table.ColumnSource;
import java.util.Map;
import java.util.Collections;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.aggregate.SlicedByteAggregateColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BooleanRollingGroupOperator extends RollingGroupOperator {
    private final SlicedByteAggregateColumnSource outputSource;

    // region extra-fields
    // endregion extra-fields

    public BooleanRollingGroupOperator(
           @NotNull final MatchPair pair,
           @NotNull final String[] affectingColumns,
           @Nullable final RowRedirection rowRedirection,
           @Nullable final String timestampColumnName,
           final long reverseWindowScaleUnits,
           final long forwardWindowScaleUnits,
           final ColumnSource<Boolean> valueSource
           // region extra-constructor-args
           // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits);

        ColumnSource<Byte> reinterpreted = (ColumnSource<Byte>) ReinterpretUtils.maybeConvertToPrimitive(valueSource);

        outputSource = timestampColumnName != null
                ? new SlicedByteAggregateColumnSource(reinterpreted, groupRowSetSource, startSource, endSource)
                // transition from revTicks (inclusive of the current row) to row offsets
                : new SlicedByteAggregateColumnSource(reinterpreted, groupRowSetSource, -reverseWindowScaleUnits + 1, forwardWindowScaleUnits);

        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}
