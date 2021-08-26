package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.JsArray;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(isNative = true, name = "dhinternal.jspb.BinaryIterator", namespace = JsPackage.GLOBAL)
public class BinaryIterator {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AllocElementsArrayUnionType {
        @JsOverlay
        static BinaryIterator.AllocElementsArrayUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Boolean asBoolean() {
            return Js.cast(this);
        }

        @JsOverlay
        default Double asDouble() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsFunction
    public interface AllocNextFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface UnionType {
            @JsOverlay
            static BinaryIterator.AllocNextFn.UnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default boolean asBoolean() {
                return Js.asBoolean(this);
            }

            @JsOverlay
            default double asDouble() {
                return Js.asDouble(this);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default boolean isBoolean() {
                return (Object) this instanceof Boolean;
            }

            @JsOverlay
            default boolean isDouble() {
                return (Object) this instanceof Double;
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }
        }

        BinaryIterator.AllocNextFn.UnionType onInvoke();
    }

    @JsFunction
    public interface BinaryIteratorNextFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface UnionType {
            @JsOverlay
            static BinaryIterator.BinaryIteratorNextFn.UnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default boolean asBoolean() {
                return Js.asBoolean(this);
            }

            @JsOverlay
            default double asDouble() {
                return Js.asDouble(this);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default boolean isBoolean() {
                return (Object) this instanceof Boolean;
            }

            @JsOverlay
            default boolean isDouble() {
                return (Object) this instanceof Double;
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }
        }

        BinaryIterator.BinaryIteratorNextFn.UnionType onInvoke();
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConstructorElementsArrayUnionType {
        @JsOverlay
        static BinaryIterator.ConstructorElementsArrayUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Boolean asBoolean() {
            return Js.cast(this);
        }

        @JsOverlay
        default Double asDouble() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetUnionType {
        @JsOverlay
        static BinaryIterator.GetUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean asBoolean() {
            return Js.asBoolean(this);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface NextUnionType {
        @JsOverlay
        static BinaryIterator.NextUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean asBoolean() {
            return Js.asBoolean(this);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    public static native BinaryIterator alloc();

    @JsOverlay
    public static final BinaryIterator alloc(
        BinaryDecoder decoder,
        BinaryIterator.AllocNextFn next,
        BinaryIterator.AllocElementsArrayUnionType[] elements) {
        return alloc(
            decoder,
            next,
            Js.<JsArray<BinaryIterator.AllocElementsArrayUnionType>>uncheckedCast(elements));
    }

    public static native BinaryIterator alloc(
        BinaryDecoder decoder,
        BinaryIterator.AllocNextFn next,
        JsArray<BinaryIterator.AllocElementsArrayUnionType> elements);

    public static native BinaryIterator alloc(BinaryDecoder decoder,
        BinaryIterator.AllocNextFn next);

    public static native BinaryIterator alloc(BinaryDecoder decoder);

    public BinaryIterator() {}

    public BinaryIterator(
        BinaryDecoder decoder,
        BinaryIterator.BinaryIteratorNextFn next,
        BinaryIterator.ConstructorElementsArrayUnionType[] elements) {}

    public BinaryIterator(
        BinaryDecoder decoder,
        BinaryIterator.BinaryIteratorNextFn next,
        JsArray<BinaryIterator.ConstructorElementsArrayUnionType> elements) {}

    public BinaryIterator(BinaryDecoder decoder, BinaryIterator.BinaryIteratorNextFn next) {}

    public BinaryIterator(BinaryDecoder decoder) {}

    public native boolean atEnd();

    public native void clear();

    public native void free();

    public native BinaryIterator.GetUnionType get();

    public native BinaryIterator.NextUnionType next();
}
