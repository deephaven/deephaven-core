package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsString;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

/**
 * Behaves like a Table, but doesn't expose all of its API for changing the internal state. Instead, state is driven by
 * the upstream table - when it changes handle, this listens and updates its own handle accordingly.
 *
 * Additionally, this is automatically subscribed to its one and only row, across all columns.
 *
 * A new config is returned any time it is accessed, to prevent accidental mutation, and to allow it to be used as a
 * template when fetching a new totals table, or changing the totals table in use.
 */
public class JsTotalsTable {
    private final JsTable wrappedTable;
    private final String directive;
    private final JsArray<JsString> groupBy;

    // unlike JsTable, we need to track the viewport since our state will change without
    // explicit API calls to this instance. This assumption changes a bit if we decide to
    // support sort/filter or custom columns on totals tables
    private Double firstRow;
    private Double lastRow;
    private Column[] columns;
    private Double updateIntervalMs;

    /**
     * Table is wrapped to let us delegate calls to it, the directive is a serialized string, and the groupBy is copied
     * when passed in, as well as when it is accessed, to prevent accidental mutation of the array.
     */
    public JsTotalsTable(JsTable wrappedTable, String directive, JsArray<JsString> groupBy) {
        this.wrappedTable = wrappedTable;
        this.directive = directive;
        this.groupBy = Js.uncheckedCast(groupBy.slice());
    }

    public void refreshViewport() {
        if (firstRow != null && lastRow != null) {
            setViewport(firstRow, lastRow, Js.uncheckedCast(columns), updateIntervalMs);
        }
    }

    @JsProperty
    public JsTotalsTableConfig getTotalsTableConfig() {
        JsTotalsTableConfig parsed = JsTotalsTableConfig.parse(directive);
        parsed.groupBy = Js.uncheckedCast(groupBy.slice());
        return parsed;
    }

    @JsMethod
    public void setViewport(double firstRow, double lastRow, @JsOptional JsArray<Column> columns,
            @JsOptional Double updateIntervalMs) {
        this.firstRow = firstRow;
        this.lastRow = lastRow;
        this.columns = columns != null ? Js.uncheckedCast(columns.slice()) : null;
        this.updateIntervalMs = updateIntervalMs;
        wrappedTable.setViewport(firstRow, lastRow, columns, updateIntervalMs);
    }

    @JsMethod
    public Promise<TableData> getViewportData() {
        return wrappedTable.getViewportData();
    }

    @JsProperty
    public JsArray<Column> getColumns() {
        return wrappedTable.getColumns();
    }

    @JsMethod
    public Column findColumn(String key) {
        return wrappedTable.findColumn(key);
    }

    @JsMethod
    public Column[] findColumns(String[] keys) {
        return wrappedTable.findColumns(keys);
    }

    @JsMethod
    public void close() {
        wrappedTable.close();
    }

    @JsProperty
    public double getSize() {
        return wrappedTable.getSize();
    }

    @Override
    public String toString() {
        return "JsTotalsTable { totalsTableConfig=" + getTotalsTableConfig() + " }";
    }

    @JsMethod
    public RemoverFn addEventListener(String name, EventFn callback) {
        return wrappedTable.addEventListener(name, callback);
    }

    @JsMethod
    public boolean removeEventListener(String name, EventFn callback) {
        return wrappedTable.removeEventListener(name, callback);
    }

    @JsMethod
    public JsArray<Sort> applySort(Sort[] sort) {
        return wrappedTable.applySort(sort);
    }

    @JsMethod
    public JsArray<CustomColumn> applyCustomColumns(Object[] customColumns) {
        return wrappedTable.applyCustomColumns(customColumns);
    }

    @JsMethod
    public JsArray<FilterCondition> applyFilter(FilterCondition[] filter) {
        return wrappedTable.applyFilter(filter);
    }

    public JsTable getWrappedTable() {
        return wrappedTable;
    }

    @JsProperty
    public JsArray<Sort> getSort() {
        return wrappedTable.getSort();
    }

    @JsProperty
    public JsArray<FilterCondition> getFilter() {
        return wrappedTable.getFilter();
    }

    @JsProperty
    public JsArray<CustomColumn> getCustomColumns() {
        return wrappedTable.getCustomColumns();
    }



}
