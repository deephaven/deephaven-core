package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.by.AggregationFactory;
import io.deephaven.engine.table.impl.select.SelectColumn;

import java.io.Serializable;

/**
 * A class that contains information required for a particular Hierarchical table type. (i.e
 * {@link Table#treeTable(String, String) tree tables} or {@link Table#rollup(AggregationFactory, SelectColumn...)
 * rollups})
 */
public interface HierarchicalTableInfo extends Serializable {
    /**
     * @return the name of the column that contains the hierarchical keys.
     */
    String getHierarchicalColumnName();

    /**
     * Sets the column formats for the table info.
     *
     * @param columnFormats the column formats to set.
     * @return a copy of this HierarchicalTableInfo with column formats
     */
    HierarchicalTableInfo withColumnFormats(String[] columnFormats);

    /**
     * Gets the column formats.
     *
     * @return the column formats, null if there are none
     */
    String[] getColumnFormats();

    /**
     * @return If this hierarchical table contains constituent rows.
     */
    boolean includesConstituents();
}
