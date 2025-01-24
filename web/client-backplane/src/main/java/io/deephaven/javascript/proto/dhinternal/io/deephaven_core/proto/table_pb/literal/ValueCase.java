//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.literal;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Literal.ValueCase",
        namespace = JsPackage.GLOBAL)
public class ValueCase {
    public static int BOOL_VALUE,
            DOUBLE_VALUE,
            LONG_VALUE,
            NANO_TIME_VALUE,
            STRING_VALUE,
            VALUE_NOT_SET;
}
