package io.deephaven.engine.plot.datasets.data;

import io.deephaven.engine.plot.errors.PlotInfo;

/**
 * An {@link IndexableData} backed by a table.
 *
 * @param <T> class of the values
 */
public abstract class LiveIndexableData<T> extends IndexableData<T> {

    /**
     * @param plotInfo plot information
     */
    public LiveIndexableData(PlotInfo plotInfo) {
        super(plotInfo);
    }

}
