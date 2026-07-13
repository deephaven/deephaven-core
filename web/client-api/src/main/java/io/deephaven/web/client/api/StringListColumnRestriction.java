//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import io.deephaven.proto.backplane.grpc.StringListRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

/**
 * A {@link ColumnRestriction} that constrains a string column to one of a fixed set of allowed values.
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
@TsName(namespace = "dh")
public class StringListColumnRestriction extends ColumnRestriction {

    private static final int TRUNCATED_LIST_LIMIT = 5;

    private final StringListRestriction restriction;

    public StringListColumnRestriction(StringListRestriction restriction) {
        super("io.deephaven.proto.backplane.grpc.StringListRestriction");
        this.restriction = restriction;
    }


    /**
     * The list of allowed string values for this column.
     *
     * @return The allowed values
     */
    @JsProperty
    public JsArray<String> getAllowedValues() {
        return JsArray.of(restriction.getAllowedValuesList().toArray(new String[0]));
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(@JsNullable Any value) {
        if (value == null) {
            return null;
        }
        return validateImpl(value.toString());
    }

    @JsNullable
    String validateImpl(String str) {
        if (restriction.getAllowedValuesList().contains(str)) {
            return null;
        }
        return "Value '" + str + "' is not in the allowed list: " + truncatedList();
    }

    private String truncatedList() {
        if (restriction.getAllowedValuesCount() <= TRUNCATED_LIST_LIMIT) {
            return String.join(", ", restriction.getAllowedValuesList());
        }
        return String.join(", ", restriction.getAllowedValuesList().subList(0, TRUNCATED_LIST_LIMIT)) + ", ...";
    }

    @Override
    public String toString() {
        return "StringListColumnRestriction{allowedValues=" + truncatedList() + "}";
    }
}

