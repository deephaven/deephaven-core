package io.deephaven.web.shared.data.treetable;

import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.SortDescriptor;
import io.deephaven.web.shared.data.TableHandle;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;

public class TreeTableRequest implements Serializable {
    public enum TreeRequestOperation {
        Expand, Contract, FilterChanged, SortChanged, Close
    }

    private long viewportStart;
    private long viewportEnd;
    private BitSet columns;

    private FilterDescriptor[] filters;
    private SortDescriptor[] sorts;

    private TableDetails[] expandedNodes;

    private int keyColumn;

    private TreeRequestOperation[] includedOps;

    private TableHandle sourceTable;

    public long getViewportStart() {
        return viewportStart;
    }

    public void setViewportStart(long viewportStart) {
        this.viewportStart = viewportStart;
    }

    public long getViewportEnd() {
        return viewportEnd;
    }

    public void setViewportEnd(long viewportEnd) {
        this.viewportEnd = viewportEnd;
    }

    public BitSet getColumns() {
        return columns;
    }

    public void setColumns(BitSet columns) {
        this.columns = columns;
    }

    public FilterDescriptor[] getFilters() {
        return filters;
    }

    public void setFilters(FilterDescriptor[] filters) {
        this.filters = filters;
    }

    public SortDescriptor[] getSorts() {
        return sorts;
    }

    public void setSorts(SortDescriptor[] sorts) {
        this.sorts = sorts;
    }

    public TableDetails[] getExpandedNodes() {
        return expandedNodes;
    }

    public void setExpandedNodes(TableDetails[] expandedNodes) {
        this.expandedNodes = expandedNodes;
    }

    public int getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(int keyColumn) {
        this.keyColumn = keyColumn;
    }

    public TreeRequestOperation[] getIncludedOps() {
        return includedOps;
    }

    public void setIncludedOps(TreeRequestOperation[] includedOps) {
        this.includedOps = includedOps;
    }

    public TableHandle getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(TableHandle sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public String toString() {
        return "TreeTableRequest{" +
                "viewportStart=" + viewportStart +
                ", viewportEnd=" + viewportEnd +
                ", columns=" + columns +
                ", filters=" + Arrays.toString(filters) +
                ", sorts=" + Arrays.toString(sorts) +
                ", expandedNodes=" + Arrays.toString(expandedNodes) +
                ", keyColumn=" + keyColumn +
                ", includedOps=" + Arrays.toString(includedOps) +
                '}';
    }
}
