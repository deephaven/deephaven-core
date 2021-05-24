package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.AxisDescriptor.AxisTypeMap",
    namespace = JsPackage.GLOBAL)
public interface AxisTypeMap {
  @JsOverlay
  static AxisTypeMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "COLOR")
  double getCOLOR();

  @JsProperty(name = "LABEL")
  double getLABEL();

  @JsProperty(name = "SHAPE")
  double getSHAPE();

  @JsProperty(name = "SIZE")
  double getSIZE();

  @JsProperty(name = "X")
  double getX();

  @JsProperty(name = "Y")
  double getY();

  @JsProperty(name = "COLOR")
  void setCOLOR(double COLOR);

  @JsProperty(name = "LABEL")
  void setLABEL(double LABEL);

  @JsProperty(name = "SHAPE")
  void setSHAPE(double SHAPE);

  @JsProperty(name = "SIZE")
  void setSIZE(double SIZE);

  @JsProperty(name = "X")
  void setX(double X);

  @JsProperty(name = "Y")
  void setY(double Y);
}
