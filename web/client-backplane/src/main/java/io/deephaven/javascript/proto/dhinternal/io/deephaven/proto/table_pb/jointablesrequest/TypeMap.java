package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.jointablesrequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.JoinTablesRequest.TypeMap",
    namespace = JsPackage.GLOBAL)
public interface TypeMap {
  @JsOverlay
  static TypeMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "CROSS_JOIN")
  double getCROSS_JOIN();

  @JsProperty(name = "EXACT_JOIN")
  double getEXACT_JOIN();

  @JsProperty(name = "LEFT_JOIN")
  double getLEFT_JOIN();

  @JsProperty(name = "NATURAL_JOIN")
  double getNATURAL_JOIN();

  @JsProperty(name = "CROSS_JOIN")
  void setCROSS_JOIN(double CROSS_JOIN);

  @JsProperty(name = "EXACT_JOIN")
  void setEXACT_JOIN(double EXACT_JOIN);

  @JsProperty(name = "LEFT_JOIN")
  void setLEFT_JOIN(double LEFT_JOIN);

  @JsProperty(name = "NATURAL_JOIN")
  void setNATURAL_JOIN(double NATURAL_JOIN);
}
