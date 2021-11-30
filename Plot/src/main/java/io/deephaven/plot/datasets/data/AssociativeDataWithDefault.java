/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.errors.PlotRuntimeException;

import java.util.Map;

/**
 * Dataset which has values associated with keys. When the dataset has no value associated with a given key, it falls
 * back to a specified default value.
 */
public class AssociativeDataWithDefault<KEY, VALUE> extends AssociativeData<KEY, VALUE> {

    private static final long serialVersionUID = -1576513511939546265L;
    private VALUE defaultValue;
    private AssociativeData<KEY, VALUE> specificValues;

    /**
     * @param plotInfo plot information
     */
    public AssociativeDataWithDefault(final PlotInfo plotInfo) {
        super(plotInfo);
    }

    /**
     * Gets the default value for this dataset.
     *
     * @return default value for this dataset
     */
    public VALUE getDefault() {
        return defaultValue;
    }

    /**
     * Defines the default value for this dataset.
     *
     * @param value default value
     */
    public void setDefault(final VALUE value) {
        this.defaultValue = value;
    }

    /**
     * Gets this dataset's key-value pairs as an {@link AssociativeData} dataset.
     *
     * @return {@link AssociativeData} dataset representing this datasets key-value pairs
     */
    public AssociativeData<KEY, VALUE> getSpecific() {
        return specificValues;
    }

    /**
     * Sets this dataset's key-value pairs.
     *
     * @param provider dataset holding key-value pairs
     */
    public void setSpecific(final AssociativeData<KEY, VALUE> provider) {
        this.specificValues = provider;
    }

    @Override
    public VALUE get(final KEY key) {
        if (specificValues == null) {
            return defaultValue;
        } else {
            final VALUE v = specificValues.get(key);
            return v == null ? defaultValue : v;
        }
    }

    @Override
    public boolean isModifiable() {
        return specificValues != null && specificValues.isModifiable();
    }

    @Override
    public void put(final KEY key, final VALUE value) {
        if (!isModifiable()) {
            throw new PlotRuntimeException("AssociativeDataWithDefault is unmodifiable", getPlotInfo());
        }

        specificValues.put(key, value);
    }

    @Override
    public <K extends KEY, V extends VALUE> void putAll(final Map<K, V> values) {
        if (!isModifiable()) {
            throw new PlotRuntimeException("AssociativeDataWithDefault is unmodifiable", getPlotInfo());
        }

        specificValues.putAll(values);
    }

    /**
     * Sets this dataset's values equal to the values in another dataset.
     *
     * @param data dataset to get values from.
     */
    public void set(final AssociativeDataWithDefault<KEY, VALUE> data) {
        this.defaultValue = data.defaultValue;
        this.specificValues = data.specificValues;
    }

}
