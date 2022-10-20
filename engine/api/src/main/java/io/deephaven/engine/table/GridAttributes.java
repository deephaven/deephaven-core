package io.deephaven.engine.table;

import io.deephaven.api.util.ConcurrentMethod;

import java.util.Map;

/**
 * Attribute-driven functionality shared by {@link Table} and other grid-like components.
 */
public interface GridAttributes<TYPE extends GridAttributes<TYPE>> extends AttributeMap<TYPE> {

    String SORTABLE_COLUMNS_ATTRIBUTE = "SortableColumns";
    String LAYOUT_HINTS_ATTRIBUTE = "LayoutHints";
    String DESCRIPTION_ATTRIBUTE = "TableDescription";
    String COLUMN_DESCRIPTIONS_ATTRIBUTE = "ColumnDescriptions";

    /**
     * Disallow sorting on all but the specified columns.
     *
     * @param allowedSortingColumns The columns for which sorting is to be allowed
     * @return A copy of this grid with the sort restrictions applied, or this if no change was needed
     */
    @ConcurrentMethod
    TYPE restrictSortTo(String... allowedSortingColumns);

    /**
     * Clear all sorting restrictions that were applied to the grid.
     *
     * @return A copy of this grid with the sort restrictions removed, or this if no change was needed
     */
    @ConcurrentMethod
    TYPE clearSortingRestrictions();

    /**
     * Apply a description to this grid.
     *
     * @param description The description to apply
     * @return A copy of this grid with the description applied, or this if no change was needed
     */
    @ConcurrentMethod
    TYPE withDescription(String description);

    /**
     * Add a description for a specific column. Users should use {@link #withColumnDescription(Map)} to set several
     * descriptions at once.
     *
     * @param column The name of the column
     * @param description The column description
     * @return A copy of this grid with the description applied, or this if no change was needed
     */
    @ConcurrentMethod
    TYPE withColumnDescription(String column, String description);

    /**
     * Add a set of column descriptions to the grid.
     *
     * @param descriptions A map of column name to column description
     * @return A copy of this grid with the descriptions applied, or this if no change was needed
     */
    @ConcurrentMethod
    TYPE withColumnDescription(Map<String, String> descriptions);

    /**
     * Set layout hints for this grid.
     *
     * @param hints A packed string of layout hints
     * @return A copy of this grid with the layout hints applied, or this if no change was needed
     */
    @ConcurrentMethod
    TYPE setLayoutHints(String hints);
}
