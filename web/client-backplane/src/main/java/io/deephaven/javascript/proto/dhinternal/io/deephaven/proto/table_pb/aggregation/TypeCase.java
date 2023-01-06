package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggregation;

import jsinterop.annotations.JsEnum;
import jsinterop.annotations.JsPackage;

@JsEnum(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.Aggregation.TypeCase",
        namespace = JsPackage.GLOBAL)
public enum TypeCase {
    COLUMNS, COUNT, FIRST_ROW_KEY, LAST_ROW_KEY, PARTITION, TYPE_NOT_SET;
}
