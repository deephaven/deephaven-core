package io.deephaven.engine.table.impl;

public class TreeTableInfo extends AbstractHierarchicalTableInfo {
    private static final long serialVersionUID = 3L;

    public final String idColumn;
    public final String parentColumn;

    public TreeTableInfo(String idColumn, String parentColumn) {
        this(idColumn, parentColumn, null);
    }

    public TreeTableInfo(String idColumn, String parentColumn, String[] columnFormats) {
        super(columnFormats);
        this.idColumn = idColumn;
        this.parentColumn = parentColumn;
    }

    @Override
    public String getHierarchicalColumnName() {
        return idColumn;
    }

    @Override
    public HierarchicalTableInfo withColumnFormats(String[] columnFormats) {
        return new TreeTableInfo(idColumn, parentColumn, columnFormats);
    }

    @Override
    public boolean includesConstituents() {
        return false;
    }
}
