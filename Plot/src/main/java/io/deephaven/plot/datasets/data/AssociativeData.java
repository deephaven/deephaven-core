/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.plot.errors.PlotInfo;

import java.io.Serializable;
import java.util.Map;

/**
 * Dataset which has values associated with keys.
 */
public abstract class AssociativeData<KEY, VALUE> implements PlotExceptionCause, Serializable {

    private final PlotInfo plotInfo;

    /**
     * @param plotInfo plot information
     */
    public AssociativeData(final PlotInfo plotInfo) {
        this.plotInfo = plotInfo;
    }

    /**
     * Gets the value associated with the key
     *
     * @param key key
     * @return value associated with the key
     */
    public abstract VALUE get(KEY key);

    /**
     * Whether the dataset is modifiable.
     *
     * @return true if the dataset is modifiable, false if not
     */
    public abstract boolean isModifiable();

    /**
     * Adds the key-value pair to the dataset.
     *
     * @param key key
     * @param value value
     */
    public abstract void put(KEY key, VALUE value);

    /**
     * Adds all key-value pairs in the map to the dataset.
     *
     * @param values keypair map
     * @param <K> type of the keys in {@code values}
     * @param <V> type of the values in {@code values}
     */
    public abstract <K extends KEY, V extends VALUE> void putAll(final Map<K, V> values);

    @Override
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }
}
