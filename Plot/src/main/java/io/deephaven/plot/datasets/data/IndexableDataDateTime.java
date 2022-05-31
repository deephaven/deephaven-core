/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.time.DateTime;
import gnu.trove.map.hash.TLongObjectHashMap;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link IndexableData} dataset with {@link DateTime} values.
 *
 * Dataset values equal to {@link io.deephaven.util.QueryConstants#NULL_LONG} are treated as null.
 */
public class IndexableDataDateTime extends IndexableData<DateTime> {
    private static final long serialVersionUID = 8122162323328323447L;
    private final long[] data;
    private final TLongObjectHashMap<DateTime> dateTimeMap = new TLongObjectHashMap<>();

    /**
     * Creates an IndexableDataDateTime instance.
     *
     * Values in {@code data} equal to {@link io.deephaven.util.QueryConstants#NULL_LONG} are treated as null.
     *
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataDateTime(long[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public DateTime get(int index) {
        final long dataValue = data[index];
        if (dataValue == NULL_LONG) {
            return null;
        }

        DateTime cachedVal = dateTimeMap.get(dataValue);

        if (cachedVal == null) {
            cachedVal = new DateTime(dataValue);
            dateTimeMap.put(dataValue, cachedVal);
        }

        return cachedVal;
    }
}
