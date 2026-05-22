//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.NonEmptyRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;

/**
 * A {@link ColumnRestriction} that requires a string column value to be non-empty.
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
@TsName(namespace = "dh")
public class NonEmptyColumnRestriction extends ColumnRestriction {

    public NonEmptyColumnRestriction() {
        super("NonEmptyRestriction");
    }

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
    public String validate(jsinterop.base.Any value) {
        if (value == null) {
            return "Value must not be null";
        }
        return validateImpl(value.toString());
    }

    @JsNullable
    String validateImpl(String value) {
        return value.isEmpty() ? "Value must not be empty" : null;
    }

    @Override
    public String toString() {
        return "NonEmptyColumnRestriction{}";
    }
}
