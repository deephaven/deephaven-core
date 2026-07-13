//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.NotNullRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;

/**
 * A {@link ColumnRestriction} that requires a column value to be non-null.
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
@TsName(namespace = "dh")
public class NotNullColumnRestriction extends ColumnRestriction {

    public NotNullColumnRestriction(NotNullRestriction ignored) {
        super("io.deephaven.proto.backplane.grpc.NotNullRestriction");
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(jsinterop.base.Any value) {
        return value == null ? "Value must not be null" : null;
    }

    @Override
    public String toString() {
        return "NotNullColumnRestriction{}";
    }
}
