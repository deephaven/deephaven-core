/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * {@link IndexableData} dataset with {@link Byte} values.
 *
 * Dataset values equal to {@link io.deephaven.util.QueryConstants#NULL_BYTE} are treated as null.
 */
public class IndexableDataByte extends IndexableData<Byte> {
    private static final long serialVersionUID = 8301093013116624033L;
    private final byte[] data;

    /**
     * Creates an IndexableDataByte instance.
     *
     * Values in {@code data} equal to {@link io.deephaven.util.QueryConstants#NULL_INT} are treated as null.
     *
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataByte(final byte[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public Byte get(int index) {
        return data[index] == NULL_BYTE ? null : data[index];
    }
}
