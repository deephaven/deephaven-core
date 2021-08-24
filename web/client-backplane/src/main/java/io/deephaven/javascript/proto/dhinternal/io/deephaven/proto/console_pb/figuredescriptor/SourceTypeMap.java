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
    int getCLOSE();

    @JsProperty(name = "COLOR")
    int getCOLOR();

    @JsProperty(name = "HIGH")
    int getHIGH();

    @JsProperty(name = "LABEL")
    int getLABEL();

    @JsProperty(name = "LOW")
    int getLOW();

    @JsProperty(name = "OPEN")
    int getOPEN();

    @JsProperty(name = "SHAPE")
    int getSHAPE();

    @JsProperty(name = "SIZE")
    int getSIZE();

    @JsProperty(name = "TIME")
    int getTIME();

    @JsProperty(name = "X")
    int getX();

    @JsProperty(name = "X_HIGH")
    int getX_HIGH();

    @JsProperty(name = "X_LOW")
    int getX_LOW();

    @JsProperty(name = "Y")
    int getY();

    @JsProperty(name = "Y_HIGH")
    int getY_HIGH();

    @JsProperty(name = "Y_LOW")
    int getY_LOW();

    @JsProperty(name = "Z")
    int getZ();

    @JsProperty(name = "CLOSE")
    void setCLOSE(int CLOSE);

    @JsProperty(name = "COLOR")
    void setCOLOR(int COLOR);

    @JsProperty(name = "HIGH")
    void setHIGH(int HIGH);

    @JsProperty(name = "LABEL")
    void setLABEL(int LABEL);

    @JsProperty(name = "LOW")
    void setLOW(int LOW);

    @JsProperty(name = "OPEN")
    void setOPEN(int OPEN);

    @JsProperty(name = "SHAPE")
    void setSHAPE(int SHAPE);

    @JsProperty(name = "SIZE")
    void setSIZE(int SIZE);

    @JsProperty(name = "TIME")
    void setTIME(int TIME);

    @JsProperty(name = "X")
    void setX(int X);

    @JsProperty(name = "X_HIGH")
    void setX_HIGH(int X_HIGH);

    @JsProperty(name = "X_LOW")
    void setX_LOW(int X_LOW);

    @JsProperty(name = "Y")
    void setY(int Y);

    @JsProperty(name = "Y_HIGH")
    void setY_HIGH(int Y_HIGH);

    @JsProperty(name = "Y_LOW")
    void setY_LOW(int Y_LOW);

    @JsProperty(name = "Z")
    void setZ(int Z);
}
