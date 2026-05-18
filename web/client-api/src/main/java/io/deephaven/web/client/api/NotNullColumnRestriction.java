//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;

/**
 * A {@link ColumnRestriction} that requires a column value to be non-null.
 */
@TsName(namespace = "dh")
public class NotNullColumnRestriction extends ColumnRestriction {

    public NotNullColumnRestriction() {
        super("NotNullRestriction", null);
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        return value == null ? "Value must not be null" : null;
    }

    @Override
    public String toString() {
        return "NotNullColumnRestriction{}";
    }
}

