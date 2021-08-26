package io.deephaven.db.v2;

import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.select.SelectColumn;

import java.io.Serializable;

/**
 * A class that contains information required for a particular Hierarchical table type. (i.e
 * {@link io.deephaven.db.tables.Table#treeTable(String, String) tree tables} or
 * {@link io.deephaven.db.tables.Table#rollup(ComboAggregateFactory, SelectColumn...) rollups})
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
