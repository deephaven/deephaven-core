/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link IndexableData} dataset with {@link Integer} values.
 *
 * Dataset values equal to {@link io.deephaven.util.QueryConstants#NULL_INT} are treated as null.
 */
public class IndexableDataInteger extends IndexableData<Integer> {
    private static final long serialVersionUID = 8301093013116624033L;
    private final int[] data;

    /**
     * Creates an IndexableDataInteger instance.
     *
     * Values in {@code data} equal to {@link io.deephaven.util.QueryConstants#NULL_INT} are treated as null.
     *
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataInteger(int[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public Integer get(int index) {
        return (index >= data.length || data[index] == NULL_INT) ? null : data[index];
    }
}
