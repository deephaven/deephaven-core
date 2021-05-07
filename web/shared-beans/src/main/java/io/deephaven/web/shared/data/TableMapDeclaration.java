package io.deephaven.web.shared.data;

import io.deephaven.web.shared.data.columns.ColumnData;

import java.io.Serializable;

/**
 * Describes a TableMap by its handle and the keys present. At this time, a key can be a String, but support for (boxed)
 * int, long, and eventually Object[] (containing only string, int, long) will be expected as well.
 */
public class TableMapDeclaration implements Serializable {
    private ColumnData keys;
    private TableMapHandle handle;

    public ColumnData getKeys() {
        return keys;
    }

    public void setKeys(ColumnData keys) {
        this.keys = keys;
    }

    public TableMapHandle getHandle() {
        return handle;
    }

    public void setHandle(TableMapHandle handle) {
        this.handle = handle;
    }
}
