package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.asofjointablesrequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.AsOfJoinTablesRequest.TypeMap",
    namespace = JsPackage.GLOBAL)
public interface TypeMap {
  @JsOverlay
  static TypeMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "AS_OF_JOIN")
  double getAS_OF_JOIN();

  @JsProperty(name = "REVERSE_AS_OF_JOIN")
  double getREVERSE_AS_OF_JOIN();

  @JsProperty(name = "AS_OF_JOIN")
  void setAS_OF_JOIN(double AS_OF_JOIN);

  @JsProperty(name = "REVERSE_AS_OF_JOIN")
  void setREVERSE_AS_OF_JOIN(double REVERSE_AS_OF_JOIN);
}
