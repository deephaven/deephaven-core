/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link AssociativeData} dataset backed by a {@link Table}. Table columns hold the keys and the associated values.
 *
 * Data types are specified in construction.
 */
public class AssociativeDataTable<KEY, VALUE, VALUECOLUMN> extends LiveAssociativeData<KEY, VALUE, VALUECOLUMN> {

    private static final long serialVersionUID = -1752085070371782144L;
    private final TableHandle tableHandle;
    private final String keyColumn;
    private final String valueColumn;
    private final Map<KEY, VALUE> data = new HashMap<>();

    /**
     * Creates an AssociativeDataSwappableTable instance.
     *
     * Key are in the {@code keyColumn} of the table held by {@code tableHandle}. Their associated values are in the
     * {@code valueColumn}.
     *
     * The data type of the keys is specified by {@code keyColumnType}. The data type of the values is specified by
     * {@code valueColumnType}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code tableHandle}, {@code keyColumn}, and
     *         {@code valueColumn} must not be null
     * @throws IllegalArgumentException {@code keyColumn} and {@code valueColumn} must be columns in
     *         {@code swappableTable}
     * @throws RuntimeException the specified data types must match the data types of the corresponding columns
     * @param tableHandle holds the underlying table
     * @param keyColumn column in the table which holds the key values
     * @param valueColumn column in the table which holds the values associated with the keys
     * @param keyColumnType data type of the keys
     * @param valueColumnType data type of the values
     * @param plotInfo plot information
     */
    public AssociativeDataTable(final TableHandle tableHandle, final String keyColumn, final String valueColumn,
            final Class<KEY> keyColumnType, final Class<VALUECOLUMN> valueColumnType, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(tableHandle, "tableHandle", getPlotInfo());
        ArgumentValidations.assertNotNull(keyColumn, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(valueColumn, "valueColumn", getPlotInfo());
        ArgumentValidations.assertColumnsInTable(tableHandle, plotInfo, keyColumn, valueColumn);
        ArgumentValidations.assertInstance(tableHandle.getTable(), keyColumn, keyColumnType,
                keyColumn + " is not of type " + keyColumnType, plotInfo);
        ArgumentValidations.assertInstance(tableHandle.getTable(), valueColumn, valueColumnType,
                valueColumn + " is not of type " + valueColumnType, plotInfo);

        this.tableHandle = tableHandle;
        tableHandle.setTable(PlotUtils.createCategoryTable(tableHandle.getTable(), new String[] {keyColumn})); // todo
                                                                                                               // should
                                                                                                               // this
                                                                                                               // be
                                                                                                               // done
                                                                                                               // here
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
        throw new PlotUnsupportedOperationException("Modifying values is not supported for AssociativeDataTable",
                getPlotInfo());
    }

    @Override
    public <K extends KEY, V extends VALUE> void putAll(Map<K, V> values) {
        throw new PlotUnsupportedOperationException("Modifying values is not supported for AssociativeDataTable",
                getPlotInfo());
    }

}
