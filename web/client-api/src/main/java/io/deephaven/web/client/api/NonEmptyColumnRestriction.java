//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.NonEmptyRestriction;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsIgnore;
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

    @JsIgnore
    public static NonEmptyColumnRestriction fromAny(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        return parseFromAny(restrictionAny, "NonEmptyRestriction",
                buffer -> {
                    NonEmptyRestriction.parseFrom(buffer);
                    return new NonEmptyColumnRestriction();
                });
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
