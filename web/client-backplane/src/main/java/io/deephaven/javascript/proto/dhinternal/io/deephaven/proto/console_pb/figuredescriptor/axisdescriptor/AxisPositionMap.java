package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.AxisDescriptor.AxisPositionMap",
    namespace = JsPackage.GLOBAL)
public interface AxisPositionMap {
  @JsOverlay
  static AxisPositionMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "BOTTOM")
  double getBOTTOM();

  @JsProperty(name = "LEFT")
  double getLEFT();

  @JsProperty(name = "NONE")
  double getNONE();

  @JsProperty(name = "RIGHT")
  double getRIGHT();

  @JsProperty(name = "TOP")
  double getTOP();

  @JsProperty(name = "BOTTOM")
  void setBOTTOM(double BOTTOM);

  @JsProperty(name = "LEFT")
  void setLEFT(double LEFT);

  @JsProperty(name = "NONE")
  void setNONE(double NONE);

  @JsProperty(name = "RIGHT")
  void setRIGHT(double RIGHT);

  @JsProperty(name = "TOP")
  void setTOP(double TOP);
}
