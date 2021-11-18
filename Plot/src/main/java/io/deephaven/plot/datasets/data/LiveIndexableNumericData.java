package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;

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
