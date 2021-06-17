package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.chartdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.ChartDescriptor.ChartTypeMap",
    namespace = JsPackage.GLOBAL)
public interface ChartTypeMap {
  @JsOverlay
  static ChartTypeMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "CATEGORY")
  int getCATEGORY();

  @JsProperty(name = "CATEGORY_3D")
  int getCATEGORY_3D();

  @JsProperty(name = "OHLC")
  int getOHLC();

  @JsProperty(name = "PIE")
  int getPIE();

  @JsProperty(name = "XY")
  int getXY();

  @JsProperty(name = "XYZ")
  int getXYZ();

  @JsProperty(name = "CATEGORY")
  void setCATEGORY(int CATEGORY);

  @JsProperty(name = "CATEGORY_3D")
  void setCATEGORY_3D(int CATEGORY_3D);

  @JsProperty(name = "OHLC")
  void setOHLC(int OHLC);

  @JsProperty(name = "PIE")
  void setPIE(int PIE);

  @JsProperty(name = "XY")
  void setXY(int XY);

  @JsProperty(name = "XYZ")
  void setXYZ(int XYZ);
}
