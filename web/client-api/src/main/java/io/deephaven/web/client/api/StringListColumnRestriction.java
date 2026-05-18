//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

/**
 * A {@link ColumnRestriction} that constrains a string column to one of a fixed set of allowed values.
 */
@TsName(namespace = "dh")
public class StringListColumnRestriction extends ColumnRestriction {

    private final JsArray<String> allowedValues;

    public StringListColumnRestriction(JsArray<String> allowedValues) {
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
        return allowedValues;
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        if (value == null) {
            return null;
        }
        String str = Js.cast(value);
        for (int i = 0; i < allowedValues.length; i++) {
            if (str.equals(allowedValues.getAt(i))) {
                return null;
            }
        }
        return "Value '" + str + "' is not in the allowed list: " + allowedValues.join(", ");
    }

    @Override
    public String toString() {
        return "StringListColumnRestriction{allowedValues=" + allowedValues.join(", ") + "}";
    }
}

