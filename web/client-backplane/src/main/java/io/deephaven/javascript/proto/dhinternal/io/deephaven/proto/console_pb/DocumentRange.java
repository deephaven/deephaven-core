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
    name = "dhinternal.io.deephaven.proto.console_pb.DocumentRange",
    namespace = JsPackage.GLOBAL)
public class DocumentRange {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StartFieldType {
            @JsOverlay
            static DocumentRange.ToObjectReturnType.StartFieldType create() {
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

        @JsOverlay
        static DocumentRange.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getEnd();

        @JsProperty
        DocumentRange.ToObjectReturnType.StartFieldType getStart();

        @JsProperty
        void setEnd(Object end);

        @JsProperty
        void setStart(DocumentRange.ToObjectReturnType.StartFieldType start);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StartFieldType {
            @JsOverlay
            static DocumentRange.ToObjectReturnType0.StartFieldType create() {
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

        @JsOverlay
        static DocumentRange.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getEnd();

        @JsProperty
        DocumentRange.ToObjectReturnType0.StartFieldType getStart();

        @JsProperty
        void setEnd(Object end);

        @JsProperty
        void setStart(DocumentRange.ToObjectReturnType0.StartFieldType start);
    }

    public static native DocumentRange deserializeBinary(Uint8Array bytes);

    public static native DocumentRange deserializeBinaryFromReader(
        DocumentRange message, Object reader);

    public static native void serializeBinaryToWriter(DocumentRange message, Object writer);

    public static native DocumentRange.ToObjectReturnType toObject(
        boolean includeInstance, DocumentRange msg);

    public native void clearEnd();

    public native void clearStart();

    public native Position getEnd();

    public native Position getStart();

    public native boolean hasEnd();

    public native boolean hasStart();

    public native Uint8Array serializeBinary();

    public native void setEnd();

    public native void setEnd(Position value);

    public native void setStart();

    public native void setStart(Position value);

    public native DocumentRange.ToObjectReturnType0 toObject();

    public native DocumentRange.ToObjectReturnType0 toObject(boolean includeInstance);
}
