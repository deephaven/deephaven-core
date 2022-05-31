/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link AssociativeData} dataset backed by a {@link HashMap}.
 */
public class AssociativeDataHashMap<KEY, VALUE> extends AssociativeData<KEY, VALUE> {

    private static final long serialVersionUID = -3038099832719606999L;
    private final Map<KEY, VALUE> values = new HashMap<>();

    /**
     * @param plotInfo plot information
     */
    public AssociativeDataHashMap(PlotInfo plotInfo) {
        super(plotInfo);
    }

    @Override
    public VALUE get(KEY key) {
        return values.get(key);
    }

    @Override
    public boolean isModifiable() {
        return true;
    }

    @Override
    public void put(KEY key, VALUE value) {
        values.put(key, value);
    }

    @Override
    public <K extends KEY, V extends VALUE> void putAll(Map<K, V> values) {
        this.values.putAll(values);
    }

}
