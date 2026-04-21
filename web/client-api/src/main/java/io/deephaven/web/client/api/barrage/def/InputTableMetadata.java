//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.def;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.ColumnRestriction;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents input table metadata parsed from the schema.
 */
public class InputTableMetadata {
    private final Map<String, ColumnRestrictions> columnRestrictions;

    @JsIgnore
    public InputTableMetadata() {
        this.columnRestrictions = new HashMap<>();
    }

    @JsIgnore
    public void addColumnRestrictions(String columnName, ColumnRestrictions restrictions) {
        columnRestrictions.put(columnName, restrictions);
    }

    @JsIgnore
    @JsNullable
    public ColumnRestrictions getColumnRestrictions(String columnName) {
        return columnRestrictions.get(columnName);
    }

    /**
     * Represents restrictions on a column's values.
     */
    public static class ColumnRestrictions {
        private final JsArray<ColumnRestriction> restrictions;

        @JsIgnore
        public ColumnRestrictions() {
            this.restrictions = new JsArray<>();
        }

        @JsIgnore
        public void addRestriction(ColumnRestriction restriction) {
            if (restriction != null) {
                restrictions.push(restriction);
            }
        }

        @JsIgnore
        public JsArray<ColumnRestriction> getRestrictions() {
            return restrictions;
        }
    }
}

