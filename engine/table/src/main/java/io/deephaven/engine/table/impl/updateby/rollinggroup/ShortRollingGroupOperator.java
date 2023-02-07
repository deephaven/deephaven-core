package io.deephaven.engine.table.impl.updateby.rollinggroup;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.sources.aggregate.SlicedShortAggregateColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

public class ShortRollingGroupOperator extends RollingGroupOperator {
    /** keep a reference to the source table column for the vector operations */
    public final ColumnSource<?> valueSource;

    /** */
    private final SlicedShortAggregateColumnSource outputSource;

    // region extra-fields
    // endregion extra-fields

    public ShortRollingGroupOperator(
           @NotNull final MatchPair pair,
           @NotNull final String[] affectingColumns,
           @Nullable final RowRedirection rowRedirection,
           @Nullable final String timestampColumnName,
           final long reverseWindowScaleUnits,
           final long forwardWindowScaleUnits,
           final ColumnSource<Short> valueSource
           // region extra-constructor-args
           // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits);
        this.valueSource = valueSource;

        // we delay assigning the group rowset, start and end sources
        outputSource = new SlicedShortAggregateColumnSource(valueSource, groupRowSetSource, startSource, endSource);

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
