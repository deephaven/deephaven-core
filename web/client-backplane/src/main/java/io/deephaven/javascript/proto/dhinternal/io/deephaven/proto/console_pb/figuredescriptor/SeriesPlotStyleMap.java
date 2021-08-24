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
    int getAREA();

    @JsProperty(name = "BAR")
    int getBAR();

    @JsProperty(name = "ERROR_BAR")
    int getERROR_BAR();

    @JsProperty(name = "HISTOGRAM")
    int getHISTOGRAM();

    @JsProperty(name = "LINE")
    int getLINE();

    @JsProperty(name = "OHLC")
    int getOHLC();

    @JsProperty(name = "PIE")
    int getPIE();

    @JsProperty(name = "SCATTER")
    int getSCATTER();

    @JsProperty(name = "STACKED_AREA")
    int getSTACKED_AREA();

    @JsProperty(name = "STACKED_BAR")
    int getSTACKED_BAR();

    @JsProperty(name = "STEP")
    int getSTEP();

    @JsProperty(name = "AREA")
    void setAREA(int AREA);

    @JsProperty(name = "BAR")
    void setBAR(int BAR);

    @JsProperty(name = "ERROR_BAR")
    void setERROR_BAR(int ERROR_BAR);

    @JsProperty(name = "HISTOGRAM")
    void setHISTOGRAM(int HISTOGRAM);

    @JsProperty(name = "LINE")
    void setLINE(int LINE);

    @JsProperty(name = "OHLC")
    void setOHLC(int OHLC);

    @JsProperty(name = "PIE")
    void setPIE(int PIE);

    @JsProperty(name = "SCATTER")
    void setSCATTER(int SCATTER);

    @JsProperty(name = "STACKED_AREA")
    void setSTACKED_AREA(int STACKED_AREA);

    @JsProperty(name = "STACKED_BAR")
    void setSTACKED_BAR(int STACKED_BAR);

    @JsProperty(name = "STEP")
    void setSTEP(int STEP);
}
