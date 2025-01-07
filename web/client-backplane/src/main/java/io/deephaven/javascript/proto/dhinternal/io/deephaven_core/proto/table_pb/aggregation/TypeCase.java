//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Aggregation.TypeCase",
        namespace = JsPackage.GLOBAL)
public class TypeCase {
    public static int COLUMNS,
            COUNT,
            FIRST_ROW_KEY,
            LAST_ROW_KEY,
            PARTITION,
            TYPE_NOT_SET;
}
