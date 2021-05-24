package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.SeriesPlotStyleMap",
    namespace = JsPackage.GLOBAL)
public interface SeriesPlotStyleMap {
  @JsOverlay
  static SeriesPlotStyleMap create() {
    return Js.uncheckedCast(JsPropertyMap.of());
  }

  @JsProperty(name = "AREA")
  double getAREA();

  @JsProperty(name = "BAR")
  double getBAR();

  @JsProperty(name = "ERROR_BAR")
  double getERROR_BAR();

  @JsProperty(name = "HISTOGRAM")
  double getHISTOGRAM();

  @JsProperty(name = "LINE")
  double getLINE();

  @JsProperty(name = "OHLC")
  double getOHLC();

  @JsProperty(name = "PIE")
  double getPIE();

  @JsProperty(name = "SCATTER")
  double getSCATTER();

  @JsProperty(name = "STACKED_AREA")
  double getSTACKED_AREA();

  @JsProperty(name = "STACKED_BAR")
  double getSTACKED_BAR();

  @JsProperty(name = "STEP")
  double getSTEP();

  @JsProperty(name = "AREA")
  void setAREA(double AREA);

  @JsProperty(name = "BAR")
  void setBAR(double BAR);

  @JsProperty(name = "ERROR_BAR")
  void setERROR_BAR(double ERROR_BAR);

  @JsProperty(name = "HISTOGRAM")
  void setHISTOGRAM(double HISTOGRAM);

  @JsProperty(name = "LINE")
  void setLINE(double LINE);

  @JsProperty(name = "OHLC")
  void setOHLC(double OHLC);

  @JsProperty(name = "PIE")
  void setPIE(double PIE);

  @JsProperty(name = "SCATTER")
  void setSCATTER(double SCATTER);

  @JsProperty(name = "STACKED_AREA")
  void setSTACKED_AREA(double STACKED_AREA);

  @JsProperty(name = "STACKED_BAR")
  void setSTACKED_BAR(double STACKED_BAR);

  @JsProperty(name = "STEP")
  void setSTEP(double STEP);
}
