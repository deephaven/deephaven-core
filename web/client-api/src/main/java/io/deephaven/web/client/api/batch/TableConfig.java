package io.deephaven.web.client.api.batch;

import io.deephaven.web.client.api.Sort;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.shared.data.CustomColumnDescriptor;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class represents a container for all the various operations you might apply to a given table (sort, filter,
 * custom columns, select distinct, soon others).
 *
 * Using this container allows us to add an optional new property without updating many methods signatures.
 */
public class TableConfig {

    private final List<Sort> sorts = new ArrayList<>();
    private final List<String> conditions = new ArrayList<>();
    private final List<String> dropColumns = new ArrayList<>();
    private final List<String> viewColumns = new ArrayList<>();
    private final List<FilterCondition> filters = new ArrayList<>();
    private final List<CustomColumnDescriptor> customColumns = new ArrayList<>();
    private final List<String> selectDistinct = new ArrayList<>();
    private boolean isFlat;

    @JsType(namespace = JsPackage.GLOBAL, name = "Object", isNative = true)
    public static class JsConfig {
        public Sort[] sorts;
        public String[] conditions;
        public String[] dropColumns;
        public String[] viewColumns;
        public FilterCondition[] filters;
        public String[] customColumns;

        public boolean isFlat;
    }

    public TableConfig() {}

    public TableConfig(
            List<Sort> sorts,
            List<String> conditions,
            List<FilterCondition> filters,
            List<CustomColumnDescriptor> customColumns,
            List<String> dropColumns,
            List<String> viewColumns) {
        this.sorts.addAll(sorts);
        this.conditions.addAll(conditions);
        this.filters.addAll(filters);
        this.customColumns.addAll(customColumns);
        this.dropColumns.addAll(dropColumns);
        this.viewColumns.addAll(viewColumns);
    }

    public List<Sort> getSorts() {
        // Need to return a copy
        return new ArrayList<>(sorts);
    }

    public List<String> getConditions() {
        // Need to return a copy
        return new ArrayList<>(conditions);
    }

    public List<String> getDropColumns() {
        // Need to return a copy
        return new ArrayList<>(dropColumns);
    }

    public List<String> getViewColumns() {
        // Need to return a copy
        return new ArrayList<>(viewColumns);
    }

    public List<FilterCondition> getFilters() {
        return new ArrayList<>(filters);
    }

    public List<CustomColumnDescriptor> getCustomColumns() {
        return new ArrayList<>(customColumns);
    }

    public List<String> getSelectDistinct() {
        return new ArrayList<>(selectDistinct);
    }

    public boolean isFlat() {
        return isFlat;
    }

    public void setFlat(boolean flat) {
        isFlat = flat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TableConfig that = (TableConfig) o;

        return isEqual(that);
    }

    public boolean isEqual(TableConfig that) {
        if (!sorts.equals(that.sorts))
            return false;
        if (!conditions.equals(that.conditions))
            return false;
        if (!dropColumns.equals(that.dropColumns))
            return false;
        if (!viewColumns.equals(that.viewColumns))
            return false;
        if (!filters.equals(that.filters))
            return false;
        if (!customColumns.equals(that.customColumns))
            return false;
        if (!selectDistinct.equals(that.selectDistinct))
            return false;
        return isFlat == that.isFlat;
    }

    @Override
    public int hashCode() {
        int result = sorts.hashCode();
        result = 31 * result + conditions.hashCode();
        result = 31 * result + dropColumns.hashCode();
        result = 31 * result + viewColumns.hashCode();
        result = 31 * result + filters.hashCode();
        result = 31 * result + customColumns.hashCode();
        result = 31 * result + selectDistinct.hashCode();
        result = 31 * result + (isFlat ? 0 : 1);
        return result;
    }

    @Override
    public String toString() {
        return "TableConfig{" +
                "sorts=" + sorts +
                ", filters=" + filters +
                ", customColumns=" + customColumns +
                ", selectDistinct=" + selectDistinct +
                ", conditions=" + conditions +
                ", dropColumns=" + dropColumns +
                ", viewColumns=" + viewColumns +
                ", isFlat=" + isFlat +
                '}';
    }

    public boolean isEmpty() {
        return sorts.isEmpty() && conditions.isEmpty() && filters.isEmpty() && customColumns.isEmpty();
    }

    protected void setSorts(List<Sort> sorts) {
        this.sorts.clear();
        this.sorts.addAll(sorts);
    }

    protected void setConditions(List<String> conditions) {
        this.conditions.clear();
        this.conditions.addAll(conditions);
    }

    protected void setDropColumns(List<String> dropColumns) {
        this.dropColumns.clear();
        this.dropColumns.addAll(dropColumns);
    }

    protected void setViewColumns(List<String> viewColumns) {
        this.viewColumns.clear();
        this.viewColumns.addAll(viewColumns);
    }

    protected void setFilters(List<FilterCondition> filters) {
        this.filters.clear();
        this.filters.addAll(filters);
    }

    protected void setCustomColumns(List<CustomColumnDescriptor> customColumns) {
        this.customColumns.clear();
        this.customColumns.addAll(customColumns);
    }

    public boolean isEmptyConfig() {
        return sorts.isEmpty() && filters.isEmpty() && customColumns.isEmpty() && selectDistinct.isEmpty();
    }

    public JsConfig toJs() {
        final JsConfig config = new JsConfig();
        config.sorts = sorts.toArray(new Sort[sorts.size()]);
        config.conditions = conditions.toArray(new String[conditions.size()]);
        config.dropColumns = dropColumns.toArray(new String[dropColumns.size()]);
        config.viewColumns = viewColumns.toArray(new String[viewColumns.size()]);
        config.filters = filters.toArray(new FilterCondition[filters.size()]);
        config.customColumns = customColumns.stream()
                .map(CustomColumnDescriptor::getExpression)
                .toArray(String[]::new);
        config.isFlat = isFlat;
        return config;
    }

    public String toSummaryString() {
        StringBuilder result = new StringBuilder();
        if (!customColumns.isEmpty()) {
            result.append("customColumns: ")
                    .append(customColumns.stream()
                            .map(CustomColumnDescriptor::getExpression)
                            .collect(Collectors.joining(",")))
                    .append("\n");
        }

        if (!filters.isEmpty()) {
            result.append("filters: ")
                    .append(filters.stream()
                            .map(FilterCondition::toString)
                            .collect(Collectors.joining(",")))
                    .append("\n");
        }

        if (!sorts.isEmpty()) {
            result.append("sorts: ")
                    .append(sorts.stream()
                            .map(s -> s.getColumn().getName() + " " + s.getDirection())
                            .collect(Collectors.joining(",")))
                    .append("\n");
        }

        if (!dropColumns.isEmpty()) {
            result.append("dropColumns: ")
                    .append(dropColumns.stream().collect(Collectors.joining(",")))
                    .append("\n");
        }

        if (!viewColumns.isEmpty()) {
            result.append("viewColumns: ")
                    .append(viewColumns.stream().collect(Collectors.joining(",")))
                    .append("\n");
        }

        if (!conditions.isEmpty()) {
            result.append("conditions: ")
                    .append(conditions.stream().collect(Collectors.joining(",")))
                    .append("\n");
        }

        if (isFlat) {
            result.append("isFlat: true \n");
        }

        return result.toString();
    }
}
