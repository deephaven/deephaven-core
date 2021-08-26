/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * {@link IndexableData} dataset with {@link Integer} values.
 *
 * Dataset values equal to {@link io.deephaven.util.QueryConstants#NULL_INT} are treated as null.
 */
public class IndexableDataCharacter extends IndexableData<Character> {
    private static final long serialVersionUID = 8122162323328323447L;
    private final char[] data;

    /**
     * Creates an IndexableDataCharacter instance.
     *
     * Values in {@code data} equal to {@link io.deephaven.util.QueryConstants#NULL_CHAR} are treated as null.
     *
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataCharacter(char[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public Character get(int index) {
        return data[index] == NULL_CHAR ? null : data[index];
    }
}
