//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.condition;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Condition.DataCase",
        namespace = JsPackage.GLOBAL)
public class DataCase {
    public static int AND,
            COMPARE,
            CONTAINS,
            DATA_NOT_SET,
            IN,
            INVOKE,
            IS_NULL,
            MATCHES,
            NOT,
            OR,
            SEARCH;
}
