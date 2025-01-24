//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.FigureDescriptor.SourceTypeMap",
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

    @JsProperty(name = "HOVER_TEXT")
    int getHOVER_TEXT();

    @JsProperty(name = "LABEL")
    int getLABEL();

    @JsProperty(name = "LOW")
    int getLOW();

    @JsProperty(name = "OPEN")
    int getOPEN();

    @JsProperty(name = "PARENT")
    int getPARENT();

    @JsProperty(name = "SHAPE")
    int getSHAPE();

    @JsProperty(name = "SIZE")
    int getSIZE();

    @JsProperty(name = "TEXT")
    int getTEXT();

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

    @JsProperty(name = "HOVER_TEXT")
    void setHOVER_TEXT(int HOVER_TEXT);

    @JsProperty(name = "LABEL")
    void setLABEL(int LABEL);

    @JsProperty(name = "LOW")
    void setLOW(int LOW);

    @JsProperty(name = "OPEN")
    void setOPEN(int OPEN);

    @JsProperty(name = "PARENT")
    void setPARENT(int PARENT);

    @JsProperty(name = "SHAPE")
    void setSHAPE(int SHAPE);

    @JsProperty(name = "SIZE")
    void setSIZE(int SIZE);

    @JsProperty(name = "TEXT")
    void setTEXT(int TEXT);

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
