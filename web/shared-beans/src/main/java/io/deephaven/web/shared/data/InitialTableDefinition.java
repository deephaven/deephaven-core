package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * A table definition constructed when using the fetch command; also includes the table id and size, which are not
 * normally part of a table definition (as they will change when the table evolves)
 */
public class InitialTableDefinition implements Serializable {

    private ColumnDefinition[] columns;
    private TableHandle id;
    private long size;
    private boolean isFlat;
    private TableAttributesDefinition attributes;

    public ColumnDefinition[] getColumns() {
        return columns;
    }

    public InitialTableDefinition setColumns(final ColumnDefinition[] columns) {
        this.columns = columns;
        return this;
    }

    public TableHandle getId() {
        return id;
    }

    public InitialTableDefinition setId(TableHandle id) {
        this.id = id;
        return this;
    }

    public long getSize() {
        return size;
    }

    public InitialTableDefinition setSize(long size) {
        this.size = size;
        return this;
    }

    public boolean isFlat() {
        return isFlat;
    }

    public InitialTableDefinition setFlat(boolean flat) {
        isFlat = flat;
        return this;
    }

    public TableAttributesDefinition getAttributes() {
        return attributes;
    }

    public InitialTableDefinition setAttributes(final TableAttributesDefinition attributes) {
        this.attributes = attributes;
        return this;
    }
}
