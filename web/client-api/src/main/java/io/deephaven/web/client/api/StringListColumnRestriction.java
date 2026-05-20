//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

import java.util.List;

/**
 * A {@link ColumnRestriction} that constrains a string column to one of a fixed set of allowed values.
 */
@TsName(namespace = "dh")
public class StringListColumnRestriction extends ColumnRestriction {

    private final List<String> allowedValues;

    public StringListColumnRestriction(List<String> allowedValues) {
        super("StringListRestriction");
        this.allowedValues = allowedValues;
    }

    /**
     * The list of allowed string values for this column.
     *
     * @return The allowed values
     */
    @JsProperty
    public JsArray<String> getAllowedValues() {
        JsArray<String> result = new JsArray<>();
        for (String value : allowedValues) {
            result.push(value);
        }
        return result;
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        if (value == null) {
            return "Value must not be null";
        }
        String str = value.toString();
        if (allowedValues.contains(str)) {
            return null;
        }
        return "Value '" + str + "' is not in the allowed list: " + String.join(", ", allowedValues);
    }

    @Override
    public String toString() {
        return "StringListColumnRestriction{allowedValues=" + String.join(", ", allowedValues) + "}";
    }
}

