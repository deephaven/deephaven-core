//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.NotNullRestriction;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;

/**
 * A {@link ColumnRestriction} that requires a column value to be non-null.
 */
@TsName(namespace = "dh")
public class NotNullColumnRestriction extends ColumnRestriction {

    public NotNullColumnRestriction() {
        super("NotNullRestriction");
    }

    @JsIgnore
    public static NotNullColumnRestriction fromAny(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        return parseFromAny(restrictionAny, "NotNullRestriction",
                buffer -> {
                    NotNullRestriction.parseFrom(buffer);
                    return new NotNullColumnRestriction();
                });
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
