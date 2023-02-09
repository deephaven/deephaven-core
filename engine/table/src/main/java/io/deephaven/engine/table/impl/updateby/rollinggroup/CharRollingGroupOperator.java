package io.deephaven.engine.table.impl.updateby.rollinggroup;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.sources.aggregate.SlicedCharAggregateColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

public class CharRollingGroupOperator extends RollingGroupOperator {
    private final SlicedCharAggregateColumnSource outputSource;

    // region extra-fields
    // endregion extra-fields

    public CharRollingGroupOperator(
           @NotNull final MatchPair pair,
           @NotNull final String[] affectingColumns,
           @Nullable final RowRedirection rowRedirection,
           @Nullable final String timestampColumnName,
           final long reverseWindowScaleUnits,
           final long forwardWindowScaleUnits,
           final ColumnSource<Character> valueSource
           // region extra-constructor-args
           // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits);

        outputSource = timestampColumnName != null
                ? new SlicedCharAggregateColumnSource(valueSource, groupRowSetSource, startSource, endSource)
                // transition from revTicks (inclusive of the current row) to row offsets
                : new SlicedCharAggregateColumnSource(valueSource, groupRowSetSource, -reverseWindowScaleUnits + 1, forwardWindowScaleUnits);

        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }

    @Override
    public void startTrackingPrev() {
        super.startTrackingPrev();
        outputSource.startTrackingPrevValues();
    }
}
