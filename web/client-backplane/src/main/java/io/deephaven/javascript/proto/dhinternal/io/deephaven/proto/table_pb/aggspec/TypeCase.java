package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec;

import jsinterop.annotations.JsEnum;
import jsinterop.annotations.JsPackage;

@JsEnum(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggSpec.TypeCase",
        namespace = JsPackage.GLOBAL)
public enum TypeCase {
    ABS_SUM, APPROXIMATE_PERCENTILE, AVG, COUNT_DISTINCT, DISTINCT, FIRST, FORMULA, FREEZE, GROUP, LAST, MAX, MEDIAN, MIN, PERCENTILE, SORTED_FIRST, SORTED_LAST, STD, SUM, TYPE_NOT_SET, T_DIGEST, UNIQUE, VAR, WEIGHTED_AVG, WEIGHTED_SUM;
}
