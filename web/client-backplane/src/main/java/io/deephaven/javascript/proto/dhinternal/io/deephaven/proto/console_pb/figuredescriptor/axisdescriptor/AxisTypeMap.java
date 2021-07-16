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
  int getCOLOR();

  @JsProperty(name = "LABEL")
  int getLABEL();

  @JsProperty(name = "SHAPE")
  int getSHAPE();

  @JsProperty(name = "SIZE")
  int getSIZE();

  @JsProperty(name = "X")
  int getX();

  @JsProperty(name = "Y")
  int getY();

  @JsProperty(name = "COLOR")
  void setCOLOR(int COLOR);

  @JsProperty(name = "LABEL")
  void setLABEL(int LABEL);

  @JsProperty(name = "SHAPE")
  void setSHAPE(int SHAPE);

  @JsProperty(name = "SIZE")
  void setSIZE(int SIZE);

  @JsProperty(name = "X")
  void setX(int X);

  @JsProperty(name = "Y")
  void setY(int Y);
}
