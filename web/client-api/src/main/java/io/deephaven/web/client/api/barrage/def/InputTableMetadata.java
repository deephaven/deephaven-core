//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.def;

import io.deephaven.web.client.api.ColumnRestriction;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents input table metadata parsed from the schema.
 */
public class InputTableMetadata {
    private final Map<String, List<ColumnRestriction>> columnRestrictions;

    @JsIgnore
    public InputTableMetadata() {
        this.columnRestrictions = new HashMap<>();
    }

    @JsIgnore
    public void addColumnRestrictions(String columnName, List<ColumnRestriction> restrictions) {
        columnRestrictions.put(columnName, restrictions);
    }

    @JsIgnore
    @JsNullable
    public List<ColumnRestriction> getColumnRestrictions(String columnName) {
        return columnRestrictions.get(columnName);
    }
}
