/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link AssociativeData} dataset backed by a {@link SwappableTable}. Table columns hold the
 * keys and associated values.
 *
 * Data types are specified in construction.
 */
public class AssociativeDataSwappableTable<KEY, VALUE, VALUECOLUMN>
    extends LiveAssociativeData<KEY, VALUE, VALUECOLUMN> {

    private static final long serialVersionUID = -8997550925581334311L;
    private final SwappableTable swappableTable;
    private final String keyColumn;
    private final String valueColumn;
    private final Map<KEY, VALUE> data = new HashMap<>();

    /**
     * Creates an AssociativeDataSwappableTable instance.
     *
     * The {@code swappableTable} must have had a lastBy applied!
     *
     * Keys are held in the {@code keyColumn} of {@code swappableTable}. Their associated values are
     * held in the {@code valueColumn}.
     *
     * The data type of the keys is specified by {@code keyColumnType}. The data type of the values
     * is specified by {@code valueColumnType}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code swappableTable},
     *         {@code keyColumn}, and {@code valueColumn} must not be null
     * @throws IllegalArgumentException {@code keyColumn} and {@code valueColumn} must be columns in
     *         {@code swappableTable}
     * @throws RuntimeException the specified data types must match the data types of the
     *         corresponding columns
     * @param swappableTable table. Must have a lastBy applied.
     * @param keyColumn column in {@code swappableTable} which holds the key values
     * @param valueColumn column in {@code swappableTable} which holds the values associated with
     *        the keys
     * @param keyColumnType data type of the keys
     * @param valueColumnType data type of the values
     * @param plotInfo plot information
     */
    public AssociativeDataSwappableTable(final SwappableTable swappableTable,
        final String keyColumn, final String valueColumn, final Class<KEY> keyColumnType,
        final Class<VALUECOLUMN> valueColumnType, final PlotInfo plotInfo) {
        super(plotInfo);
        this.swappableTable = swappableTable;
        ArgumentValidations.assertNotNull(swappableTable, "swappableTable", getPlotInfo());
        ArgumentValidations.assertNotNull(keyColumn, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(valueColumn, "valueColumn", getPlotInfo());
        ArgumentValidations.assertColumnsInTable(swappableTable.getTableDefinition(), plotInfo,
            keyColumn, valueColumn);
        ArgumentValidations.assertInstance(swappableTable.getTableDefinition(), keyColumn,
            keyColumnType, keyColumn + " is not of type " + keyColumnType, plotInfo);
        ArgumentValidations.assertInstance(swappableTable.getTableDefinition(), valueColumn,
            valueColumnType, valueColumn + " is not of type " + valueColumnType, plotInfo);

        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
    }


    ////////////////////////// external //////////////////////////


    @Override
    public VALUE get(KEY key) {
        return data.get(key);
    }

    @Override
    public boolean isModifiable() {
        return false;
    }

    @Override
    public void put(KEY key, VALUE value) {
        throw new PlotUnsupportedOperationException(
            "Modifying values is not supported for AssociativeDataSwappableTable", getPlotInfo());
    }

    @Override
    public <K extends KEY, V extends VALUE> void putAll(Map<K, V> values) {
        throw new PlotUnsupportedOperationException(
            "Modifying values is not supported for AssociativeDataSwappableTable", getPlotInfo());
    }
}
