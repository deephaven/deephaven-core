package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;

/**
 * An {@link IndexableNumericData} backed by a table.
 */
public abstract class LiveIndexableNumericData extends IndexableNumericData {

    /**
     * @param plotInfo plot information
     */
    public LiveIndexableNumericData(PlotInfo plotInfo) {
        super(plotInfo);
    }
}
