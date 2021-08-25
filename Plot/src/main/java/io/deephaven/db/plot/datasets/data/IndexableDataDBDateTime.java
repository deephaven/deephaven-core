/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.tables.utils.DBDateTime;
import gnu.trove.map.hash.TLongObjectHashMap;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link IndexableData} dataset with {@link DBDateTime} values.
 *
 * Dataset values equal to {@link io.deephaven.util.QueryConstants#NULL_LONG} are treated as null.
 */
public class IndexableDataDBDateTime extends IndexableData<DBDateTime> {
    private static final long serialVersionUID = 8122162323328323447L;
    private final long[] data;
    private final TLongObjectHashMap<DBDateTime> dbDateTimeMap = new TLongObjectHashMap<>();

    /**
     * Creates an IndexableDataDBDateTime instance.
     *
     * Values in {@code data} equal to {@link io.deephaven.util.QueryConstants#NULL_LONG} are
     * treated as null.
     *
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataDBDateTime(long[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public DBDateTime get(int index) {
        final long dataValue = data[index];
        if (dataValue == NULL_LONG) {
            return null;
        }

        DBDateTime cachedVal = dbDateTimeMap.get(dataValue);

        if (cachedVal == null) {
            cachedVal = new DBDateTime(dataValue);
            dbDateTimeMap.put(dataValue, cachedVal);
        }

        return cachedVal;
    }
}
