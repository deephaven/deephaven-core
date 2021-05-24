package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.SourceTypeMap",
    namespace = JsPackage.GLOBAL)
public interface SourceTypeMap {
  @JsOverlay
  static SourceTypeMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "CLOSE")
  double getCLOSE();

  @JsProperty(name = "COLOR")
  double getCOLOR();

  @JsProperty(name = "HIGH")
  double getHIGH();

  @JsProperty(name = "LABEL")
  double getLABEL();

  @JsProperty(name = "LOW")
  double getLOW();

  @JsProperty(name = "OPEN")
  double getOPEN();

  @JsProperty(name = "SHAPE")
  double getSHAPE();

  @JsProperty(name = "SIZE")
  double getSIZE();

  @JsProperty(name = "TIME")
  double getTIME();

  @JsProperty(name = "X")
  double getX();

  @JsProperty(name = "X_HIGH")
  double getX_HIGH();

  @JsProperty(name = "X_LOW")
  double getX_LOW();

  @JsProperty(name = "Y")
  double getY();

  @JsProperty(name = "Y_HIGH")
  double getY_HIGH();

  @JsProperty(name = "Y_LOW")
  double getY_LOW();

  @JsProperty(name = "Z")
  double getZ();

  @JsProperty(name = "CLOSE")
  void setCLOSE(double CLOSE);

  @JsProperty(name = "COLOR")
  void setCOLOR(double COLOR);

  @JsProperty(name = "HIGH")
  void setHIGH(double HIGH);

  @JsProperty(name = "LABEL")
  void setLABEL(double LABEL);

  @JsProperty(name = "LOW")
  void setLOW(double LOW);

  @JsProperty(name = "OPEN")
  void setOPEN(double OPEN);

  @JsProperty(name = "SHAPE")
  void setSHAPE(double SHAPE);

  @JsProperty(name = "SIZE")
  void setSIZE(double SIZE);

  @JsProperty(name = "TIME")
  void setTIME(double TIME);

  @JsProperty(name = "X")
  void setX(double X);

  @JsProperty(name = "X_HIGH")
  void setX_HIGH(double X_HIGH);

  @JsProperty(name = "X_LOW")
  void setX_LOW(double X_LOW);

  @JsProperty(name = "Y")
  void setY(double Y);

  @JsProperty(name = "Y_HIGH")
  void setY_HIGH(double Y_HIGH);

  @JsProperty(name = "Y_LOW")
  void setY_LOW(double Y_LOW);

  @JsProperty(name = "Z")
  void setZ(double Z);
}
