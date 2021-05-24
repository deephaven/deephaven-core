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
  double getCATEGORY();

  @JsProperty(name = "CATEGORY_3D")
  double getCATEGORY_3D();

  @JsProperty(name = "OHLC")
  double getOHLC();

  @JsProperty(name = "PIE")
  double getPIE();

  @JsProperty(name = "XY")
  double getXY();

  @JsProperty(name = "XYZ")
  double getXYZ();

  @JsProperty(name = "CATEGORY")
  void setCATEGORY(double CATEGORY);

  @JsProperty(name = "CATEGORY_3D")
  void setCATEGORY_3D(double CATEGORY_3D);

  @JsProperty(name = "OHLC")
  void setOHLC(double OHLC);

  @JsProperty(name = "PIE")
  void setPIE(double PIE);

  @JsProperty(name = "XY")
  void setXY(double XY);

  @JsProperty(name = "XYZ")
  void setXYZ(double XYZ);
}
