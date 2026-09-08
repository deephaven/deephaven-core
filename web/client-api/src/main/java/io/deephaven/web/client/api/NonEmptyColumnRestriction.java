//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.NonEmptyRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.base.Any;

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

    public NonEmptyColumnRestriction(NonEmptyRestriction ignored) {
        super("io.deephaven.proto.backplane.grpc.NonEmptyRestriction");
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
    String validateImpl(String value) {
        return value.isEmpty() ? "Value must not be empty" : null;
    }

    @Override
    public String toString() {
        return "NonEmptyColumnRestriction{}";
    }
}
