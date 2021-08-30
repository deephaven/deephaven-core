package io.deephaven.web.shared.data;

import java.io.Serializable;

public class RollupDefinition implements Serializable {
    public enum LeafType {
        Normal, Constituent
    }

    private LeafType leafType;

    private String[] byColumns;

    private String[] sourceColumnNames;
    private String[] rollupColumnNames;

    public LeafType getLeafType() {
        return leafType;
    }

    public void setLeafType(final LeafType leafType) {
        this.leafType = leafType;
    }

    public String[] getByColumns() {
        return byColumns;
    }

    public void setByColumns(final String[] byColumns) {
        this.byColumns = byColumns;
    }

    public String[] getSourceColumnNames() {
        return sourceColumnNames;
    }

    public void setSourceColumnNames(final String[] sourceColumnNames) {
        this.sourceColumnNames = sourceColumnNames;
    }

    public String[] getRollupColumnNames() {
        return rollupColumnNames;
    }

    public void setRollupColumnNames(final String[] rollupColumnNames) {
        this.rollupColumnNames = rollupColumnNames;
    }
}
