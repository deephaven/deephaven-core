package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest.operation;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.BatchTableRequest.Operation.OpCase",
    namespace = JsPackage.GLOBAL)
public class OpCase {
  public static int COMBO_AGGREGATE,
  DROP_COLUMNS,
  EMPTY_TABLE,
  FILTER,
  FLATTEN,
  HEAD,
  HEAD_BY,
  JOIN,
  LAZY_UPDATE,
  MERGE,
  OP_NOT_SET,
  SELECT,
  SELECT_DISTINCT,
  SNAPSHOT,
  SORT,
  TAIL,
  TAIL_BY,
  TIME_TABLE,
  UNGROUP,
  UNSTRUCTURED_FILTER,
  UPDATE,
  UPDATE_VIEW,
  VIEW;
}
