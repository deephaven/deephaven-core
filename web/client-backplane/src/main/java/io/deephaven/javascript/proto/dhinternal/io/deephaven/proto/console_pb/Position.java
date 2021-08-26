package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.Position",
        namespace = JsPackage.GLOBAL)
public class Position {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static Position.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCharacter();

        @JsProperty
        double getLine();

        @JsProperty
        void setCharacter(double character);

        @JsProperty
        void setLine(double line);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static Position.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCharacter();

        @JsProperty
        double getLine();

        @JsProperty
        void setCharacter(double character);

        @JsProperty
        void setLine(double line);
    }

    public static native Position deserializeBinary(Uint8Array bytes);

    public static native Position deserializeBinaryFromReader(Position message, Object reader);

    public static native void serializeBinaryToWriter(Position message, Object writer);

    public static native Position.ToObjectReturnType toObject(boolean includeInstance, Position msg);

    public native double getCharacter();

    public native double getLine();

    public native Uint8Array serializeBinary();

    public native void setCharacter(double value);

    public native void setLine(double value);

    public native Position.ToObjectReturnType0 toObject();

    public native Position.ToObjectReturnType0 toObject(boolean includeInstance);
}
