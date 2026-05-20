//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;

/**
 * A {@link ColumnRestriction} that requires a string column value to be non-empty.
 */
@TsName(namespace = "dh")
public class NonEmptyColumnRestriction extends ColumnRestriction {

    public NonEmptyColumnRestriction() {
        super("NonEmptyRestriction");
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        if (value == null) {
            return "Value must not be null";
        }
        return value.toString().isEmpty() ? "Value must not be empty" : null;
    }

    @Override
    public String toString() {
        return "NonEmptyColumnRestriction{}";
    }
}

