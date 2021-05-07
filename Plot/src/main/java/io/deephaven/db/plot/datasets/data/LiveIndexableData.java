package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;

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
