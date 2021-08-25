package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.protocol.Flight_pb.Criteria",
        namespace = JsPackage.GLOBAL)
public class Criteria {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetExpressionUnionType {
        @JsOverlay
        static Criteria.GetExpressionUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetExpressionValueUnionType {
        @JsOverlay
        static Criteria.SetExpressionValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetExpressionUnionType {
            @JsOverlay
            static Criteria.ToObjectReturnType.GetExpressionUnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static Criteria.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Criteria.ToObjectReturnType.GetExpressionUnionType getExpression();

        @JsProperty
        void setExpression(Criteria.ToObjectReturnType.GetExpressionUnionType expression);

        @JsOverlay
        default void setExpression(String expression) {
            setExpression(
                    Js.<Criteria.ToObjectReturnType.GetExpressionUnionType>uncheckedCast(expression));
        }

        @JsOverlay
        default void setExpression(Uint8Array expression) {
            setExpression(
                    Js.<Criteria.ToObjectReturnType.GetExpressionUnionType>uncheckedCast(expression));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetExpressionUnionType {
            @JsOverlay
            static Criteria.ToObjectReturnType0.GetExpressionUnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static Criteria.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Criteria.ToObjectReturnType0.GetExpressionUnionType getExpression();

        @JsProperty
        void setExpression(Criteria.ToObjectReturnType0.GetExpressionUnionType expression);

        @JsOverlay
        default void setExpression(String expression) {
            setExpression(
                    Js.<Criteria.ToObjectReturnType0.GetExpressionUnionType>uncheckedCast(expression));
        }

        @JsOverlay
        default void setExpression(Uint8Array expression) {
            setExpression(
                    Js.<Criteria.ToObjectReturnType0.GetExpressionUnionType>uncheckedCast(expression));
        }
    }

    public static native Criteria deserializeBinary(Uint8Array bytes);

    public static native Criteria deserializeBinaryFromReader(Criteria message, Object reader);

    public static native void serializeBinaryToWriter(Criteria message, Object writer);

    public static native Criteria.ToObjectReturnType toObject(boolean includeInstance, Criteria msg);

    public native Criteria.GetExpressionUnionType getExpression();

    public native String getExpression_asB64();

    public native Uint8Array getExpression_asU8();

    public native Uint8Array serializeBinary();

    public native void setExpression(Criteria.SetExpressionValueUnionType value);

    @JsOverlay
    public final void setExpression(String value) {
        setExpression(Js.<Criteria.SetExpressionValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setExpression(Uint8Array value) {
        setExpression(Js.<Criteria.SetExpressionValueUnionType>uncheckedCast(value));
    }

    public native Criteria.ToObjectReturnType0 toObject();

    public native Criteria.ToObjectReturnType0 toObject(boolean includeInstance);
}
