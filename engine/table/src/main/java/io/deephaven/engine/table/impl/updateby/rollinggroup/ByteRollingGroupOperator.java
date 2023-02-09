/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingGroupOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollinggroup;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.sources.aggregate.SlicedByteAggregateColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

public class ByteRollingGroupOperator extends RollingGroupOperator {
    private final SlicedByteAggregateColumnSource outputSource;

    // region extra-fields
    // endregion extra-fields

    public ByteRollingGroupOperator(
           @NotNull final MatchPair pair,
           @NotNull final String[] affectingColumns,
           @Nullable final RowRedirection rowRedirection,
           @Nullable final String timestampColumnName,
           final long reverseWindowScaleUnits,
           final long forwardWindowScaleUnits,
           final ColumnSource<Byte> valueSource
           // region extra-constructor-args
           // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits);

        outputSource = timestampColumnName != null
                ? new SlicedByteAggregateColumnSource(valueSource, groupRowSetSource, startSource, endSource)
                // transition from revTicks (inclusive of the current row) to row offsets
                : new SlicedByteAggregateColumnSource(valueSource, groupRowSetSource, -reverseWindowScaleUnits + 1, forwardWindowScaleUnits);

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
