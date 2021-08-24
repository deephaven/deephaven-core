package io.deephaven.web.shared.requests;

import io.deephaven.web.shared.data.TableHandle;

import java.io.Serializable;
import java.util.Arrays;

public class RollupTableRequest implements Serializable {
    private TableHandle table;
    private TableHandle resultHandle;
    private String[] groupingColumns;
    private String[] aggregations;
    private boolean includeConstituents;
    private boolean includeOriginalColumns;
    private boolean includeDescriptions;

    public RollupTableRequest() {

    }

    public TableHandle getTable() {
        return table;
    }

    public void setTable(TableHandle table) {
        this.table = table;
    }

    public TableHandle getResultHandle() {
        return resultHandle;
    }

    public void setResultHandle(TableHandle resultHandle) {
        this.resultHandle = resultHandle;
    }

    public String[] getGroupingColumns() {
        return groupingColumns;
    }

    public void setGroupingColumns(String[] groupingColumns) {
        this.groupingColumns = groupingColumns;
    }

    public boolean isIncludeConstituents() {
        return includeConstituents;
    }

    public void setIncludeConstituents(boolean includeConstituents) {
        this.includeConstituents = includeConstituents;
    }

    public boolean isIncludeOriginalColumns() {
        return includeOriginalColumns;
    }

    public void setIncludeOriginalColumns(boolean includeOriginalColumns) {
        this.includeOriginalColumns = includeOriginalColumns;
    }

    public boolean isIncludeDescriptions() {
        return includeDescriptions;
    }

    public void setIncludeDescriptions(boolean includeDescriptions) {
        this.includeDescriptions = includeDescriptions;
    }

    public String[] getAggregations() {
        return aggregations;
    }

    public void setAggregations(String[] aggregations) {
        this.aggregations = aggregations;
    }

    @Override
    public String toString() {
        return "RollupTableRequest{" +
            "table=" + table +
            ", resultHandle=" + resultHandle +
            ", groupingColumns=" + Arrays.toString(groupingColumns) +
            ", aggregations=" + Arrays.toString(aggregations) +
            ", includeConstituents=" + includeConstituents +
            ", includeOriginalColumns=" + includeOriginalColumns +
            '}';
    }
}
