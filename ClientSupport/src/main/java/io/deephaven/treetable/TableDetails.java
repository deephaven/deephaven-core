package io.deephaven.treetable;

import io.deephaven.db.tables.Table;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * A basic description of a particular table within a Tree Table.
 * </p>
 *
 * <p>
 * When this structure is created, if the table id is included then the query will fetch the table
 * from it's parent by the key, applying any filters and sorts required. Before data is returned to
 * the client, the set of children is updated to reflect any changes in the table.
 * </p>
 */
public class TableDetails implements Serializable {
    private static final long serialVersionUID = 2L;

    private final Object key;
    private final Set<Object> children;
    private transient Table table;

    /** This is for TSQ use only */
    private transient boolean removed;

    public TableDetails(Object key, Set<Object> children) {
        this.key = key;
        this.children = new HashSet<>(children);
    }

    public Object getKey() {
        return key;
    }

    public Set<Object> getChildren() {
        return children;
    }

    Table getTable() {
        return table;
    }

    void setTable(Table table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "{key=" + key + ", children=" + children + ", table=" + table
            + (removed ? ", REMOVED" : "") + "}";
    }

    public TableDetails copy() {
        return new TableDetails(key, children);
    }

    boolean isRemoved() {
        return removed;
    }

    void setRemoved(boolean removed) {
        this.removed = removed;
    }
}
