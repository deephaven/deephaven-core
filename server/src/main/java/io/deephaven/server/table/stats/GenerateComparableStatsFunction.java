/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.server.table.stats;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

public class GenerateComparableStatsFunction {

    /*
     * Defines how many unique values in a given column will be listed in a tooltip before it collapses down to just
     * showing a count of unique values, instead of the count for each value. This is the same configuration property as
     * in GuiTable.
     */
    private static final String PROP_TOOLTIP_MAX_ROW_COUNT = "GuiTable.tooltipMaxItemCount";
    private static final int PROP_TOOLTIP_MAX_ROW_COUNT_DEFAULT = 20;

    public static final int MAX_UNIQUE_VALUES_TO_PRINT = Configuration.getInstance()
            .getIntegerWithDefault(PROP_TOOLTIP_MAX_ROW_COUNT, PROP_TOOLTIP_MAX_ROW_COUNT_DEFAULT);

    private final String columnName;

    public GenerateComparableStatsFunction(final String columnName) {
        this.columnName = columnName;
    }

    public Table call(final Table table) {
        final Mutable<Table> resultHolder = new MutableObject<>();

        ConstructSnapshot.callDataSnapshotFunction("GenerateComparableStats()",
                ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                (usePrev, beforeClockValue) -> {
                    resultHolder.setValue(ChunkedComparableStatsKernel
                            .getChunkedComparableStats(MAX_UNIQUE_VALUES_TO_PRINT, table, columnName, usePrev));
                    return true;
                });

        return resultHolder.getValue();
    }

    public String toString() {
        return "GenerateComparableStatsFunction{columnName='" + columnName + "'}";
    }
}
