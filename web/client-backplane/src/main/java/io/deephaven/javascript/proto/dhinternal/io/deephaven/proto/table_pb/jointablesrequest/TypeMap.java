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

  @JsProperty(name = "ASOFJOIN")
  double getASOFJOIN();

  @JsProperty(name = "CROSSJOIN")
  double getCROSSJOIN();

  @JsProperty(name = "EXACTJOIN")
  double getEXACTJOIN();

  @JsProperty(name = "LEFTJOIN")
  double getLEFTJOIN();

  @JsProperty(name = "NATURALJOIN")
  double getNATURALJOIN();

  @JsProperty(name = "REVERSEASOFJOIN")
  double getREVERSEASOFJOIN();

  @JsProperty(name = "ASOFJOIN")
  void setASOFJOIN(double ASOFJOIN);

  @JsProperty(name = "CROSSJOIN")
  void setCROSSJOIN(double CROSSJOIN);

  @JsProperty(name = "EXACTJOIN")
  void setEXACTJOIN(double EXACTJOIN);

  @JsProperty(name = "LEFTJOIN")
  void setLEFTJOIN(double LEFTJOIN);

  @JsProperty(name = "NATURALJOIN")
  void setNATURALJOIN(double NATURALJOIN);

  @JsProperty(name = "REVERSEASOFJOIN")
  void setREVERSEASOFJOIN(double REVERSEASOFJOIN);
}
