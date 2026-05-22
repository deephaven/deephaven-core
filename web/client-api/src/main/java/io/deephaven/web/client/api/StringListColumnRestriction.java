//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import io.deephaven.proto.backplane.grpc.StringListRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

import java.util.List;

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

    private final List<String> allowedValues;

    public StringListColumnRestriction(List<String> allowedValues) {
        super("StringListRestriction");
        this.allowedValues = allowedValues;
    }

    public static StringListColumnRestriction fromAny(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        return parseFromAny(restrictionAny, "StringListRestriction",
                buffer -> new StringListColumnRestriction(
                        StringListRestriction.parseFrom(buffer).getAllowedValuesList()));
    }

    /**
     * The list of allowed string values for this column.
     *
     * @return The allowed values
     */
    @JsProperty
    public JsArray<String> getAllowedValues() {
        return JsArray.of(allowedValues.toArray(new String[0]));
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
        return "Value '" + str + "' is not in the allowed list: " + truncatedList();
    }

    private String truncatedList() {
        if (allowedValues.size() <= TRUNCATED_LIST_LIMIT) {
            return String.join(", ", allowedValues);
        }
        return String.join(", ", allowedValues.subList(0, TRUNCATED_LIST_LIMIT)) + ", ...";
    }

    @Override
    public String toString() {
        return "StringListColumnRestriction{allowedValues=" + truncatedList() + "}";
    }
}

