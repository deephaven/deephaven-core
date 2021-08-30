package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;

/**
 * An {@link AssociativeData} backed by a table.
 *
 * @param <KEY> class of the keys
 * @param <VALUE> class of the values
 * @param <VALUECOLUMN> class of the value column in the table. Will be converted to VALUE
 */
public abstract class LiveAssociativeData<KEY, VALUE, VALUECOLUMN> extends AssociativeData<KEY, VALUE> {
    /**
     * @param plotInfo plot information
     */
    public LiveAssociativeData(PlotInfo plotInfo) {
        super(plotInfo);
    }

    // allow users to convert between the value in the column and the return value of the provider
    public VALUE convert(VALUECOLUMN v) {
        return (VALUE) v;
    }
}
