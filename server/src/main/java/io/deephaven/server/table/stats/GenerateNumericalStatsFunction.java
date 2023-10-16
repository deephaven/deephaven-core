/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.server.table.stats;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

public class GenerateNumericalStatsFunction {

    private final String columnName;

    public GenerateNumericalStatsFunction(final String columnName) {
        this.columnName = columnName;
    }

    public Table call(final Table table) {
        final Mutable<Table> resultHolder = new MutableObject<>();

        ConstructSnapshot.callDataSnapshotFunction("GenerateNumericalStats()",
                ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                (usePrev, beforeClockValue) -> {
                    resultHolder
                            .setValue(ChunkedNumericalStatsKernel.getChunkedNumericalStats(table, columnName, usePrev));
                    return true;
                });

        return resultHolder.getValue();
    }

    @Override
    public String toString() {
        return "GenerateNumericalStatsFunction{columnName='" + columnName + "'}";
    }
}
