package io.deephaven.web.client.api.barrage.def;

/**
 * A table definition constructed when using the fetch command; also includes the table id and size, which are not
 * normally part of a table definition (as they will change when the table evolves)
 */
public class InitialTableDefinition {

    private ColumnDefinition[] columns;
    private TableAttributesDefinition attributes;

    public ColumnDefinition[] getColumns() {
        return columns;
    }

    public InitialTableDefinition setColumns(final ColumnDefinition[] columns) {
        this.columns = columns;
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
