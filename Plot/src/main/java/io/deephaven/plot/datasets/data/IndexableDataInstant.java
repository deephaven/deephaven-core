//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;
import gnu.trove.map.hash.TLongObjectHashMap;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link IndexableData} dataset with {@link Instant} values.
 *
 * Dataset values equal to {@link io.deephaven.util.QueryConstants#NULL_LONG} are treated as null.
 */
public class IndexableDataInstant extends IndexableData<Instant> {
    private static final long serialVersionUID = 8122162323328323447L;
    private final long[] data;
    private final TLongObjectHashMap<Instant> instantMap = new TLongObjectHashMap<>();

    /**
     * Creates an IndexableDataInstant instance.
     *
     * Values in {@code data} equal to {@link io.deephaven.util.QueryConstants#NULL_LONG} are treated as null.
     *
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataInstant(long[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public Instant get(int index) {
        final long dataValue = data[index];
        if (dataValue == NULL_LONG) {
            return null;
        }

        Instant cachedVal = instantMap.get(dataValue);

        if (cachedVal == null) {
            cachedVal = DateTimeUtils.epochNanosToInstant(dataValue);
            instantMap.put(dataValue, cachedVal);
        }

        return cachedVal;
    }
}
