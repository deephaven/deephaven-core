//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto;

import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.BadDataBehaviorMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.CaseSensitivityMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.MatchTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.NullValueMap;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb",
        namespace = JsPackage.GLOBAL)
public class Table_pb {
    public static BadDataBehaviorMap BadDataBehavior;
    public static CaseSensitivityMap CaseSensitivity;
    public static MatchTypeMap MatchType;
    public static NullValueMap NullValue;
}
