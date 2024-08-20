//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.Table;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Helper class for managing the sorted columns attribute.
 */
public class SortedColumnsAttribute {
    private SortedColumnsAttribute() {}

    /**
     * Retrieve the sorting order for a column from an attribute.
     *
     * @param attribute the Table's value of {@link Table#SORTED_COLUMNS_ATTRIBUTE}.
     * @param columnName the name of the column
     *
     * @return an optional containing the SortingOrder of the column if defined, empty otherwise
     */
    public static Optional<SortingOrder> getOrderForColumn(String attribute, String columnName) {
        return Optional.ofNullable(stringToMap(attribute, false).get(columnName));
    }

    /**
     * Retrieve the sorting order for a column from a table.
     *
     * @param table the table to interrogate
     * @param columnName the name of the column
     *
     * @return an optional containing the SortingOrder of the column if defined, empty otherwise
     */
    public static Optional<SortingOrder> getOrderForColumn(Table table, String columnName) {
        return getOrderForColumn((String) table.getAttribute(Table.SORTED_COLUMNS_ATTRIBUTE), columnName);
    }

    /**
     * Determine if a table is sorted by the given column and order.
     *
     * @param table the table to interrogate
     * @param columnName the name of the column
     * @param order the order to check for
     *
     * @return true if table is sorted by columnName in the specified order
     */
    public static boolean isSortedBy(Table table, String columnName, SortingOrder order) {
        final Optional<SortingOrder> orderForColumn = getOrderForColumn(table, columnName);
        return orderForColumn.filter(sortingOrder -> sortingOrder == order).isPresent();
    }

    /**
     * Pack the desired sorting order into a String attribute.
     *
     * @param attribute an attribute containing sorting order
     * @param columnName the column to update
     * @param order the order that the column is sorted in
     * @return a String suitable for use as a {@link Table#SORTED_COLUMNS_ATTRIBUTE} value.
     */
    public static String setOrderForColumn(String attribute, String columnName, SortingOrder order) {
        Map<String, SortingOrder> map = stringToMap(attribute, true);
        if (Objects.equals(map.get(columnName), order)) {
            return attribute;
        }
        map.put(columnName, order);
        return stringFromMap(map);
    }

    /**
     * Mark the table as sorted by the given column.
     *
     * @param table the table to update
     * @param columnName the column to update
     * @param order the order that the column is sorted in
     */
    public static void setOrderForColumn(BaseTable<?> table, String columnName, SortingOrder order) {
        table.setAttribute(Table.SORTED_COLUMNS_ATTRIBUTE,
                (final Object oldAttribute) -> setOrderForColumn((String) oldAttribute, columnName, order));
    }

    /**
     * Ensure that the result table is marked as sorted by the given column.
     *
     * @param table the table to update
     * @param columnName the column to update
     * @param order the order that the column is sorted in
     * @return {@code table}, or a copy of it with the necessary attribute set
     */
    public static Table withOrderForColumn(Table table, String columnName, SortingOrder order) {
        final String oldAttribute = (String) table.getAttribute(Table.SORTED_COLUMNS_ATTRIBUTE);
        final String newAttribute = setOrderForColumn(oldAttribute, columnName, order);
        return table.withAttributes(Map.of(Table.SORTED_COLUMNS_ATTRIBUTE, newAttribute));
    }

    /**
     * Get the columns a {@link Table} is sorted by.
     *
     * @param table The table to interrogate
     *
     * @return A (possibly-empty) list of {@link SortColumn SortColumns} representing columns the table is sorted on and
     *         their associated sort order
     */
    public static List<SortColumn> getSortedColumns(Table table) {
        final String attribute = (String) table.getAttribute(Table.SORTED_COLUMNS_ATTRIBUTE);
        if (attribute == null || attribute.isEmpty()) {
            return Collections.emptyList();
        }
        return stringToMap(attribute, true).entrySet().stream().map(e -> {
            final ColumnName columnName = ColumnName.of(e.getKey());
            return e.getValue().isAscending() ? SortColumn.asc(columnName) : SortColumn.desc(columnName);
        }).collect(Collectors.toList());
    }

    private static Map<String, SortingOrder> stringToMap(String attribute, boolean writable) {
        if (attribute == null || attribute.isEmpty()) {
            return writable ? new HashMap<>() : Collections.emptyMap();
        }
        final String[] columnAttrs = attribute.split(",");

        Map<String, SortingOrder> map = Arrays.stream(columnAttrs).map(s -> s.split("="))
                .collect(Collectors.toMap(a -> a[0], a -> SortingOrder.valueOf(a[1])));
        if (writable) {
            return map;
        } else {
            return Collections.unmodifiableMap(map);
        }
    }

    private static String stringFromMap(Map<String, SortingOrder> map) {
        if (map.isEmpty()) {
            return null;
        }
        return map.entrySet().stream().map(x -> x.getKey() + "=" + x.getValue()).collect(Collectors.joining(","));
    }
}
