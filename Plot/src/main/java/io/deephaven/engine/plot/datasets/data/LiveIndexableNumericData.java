package io.deephaven.engine.plot.datasets.data;

import io.deephaven.engine.plot.errors.PlotInfo;

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
