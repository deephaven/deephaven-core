package io.deephaven.javascript.proto.dhinternal.browserheaders;

import elemental2.core.JsArray;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.browserHeaders.BrowserHeaders",
        namespace = JsPackage.GLOBAL)
public class BrowserHeaders {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AppendValueUnionType {
        @JsOverlay
        static BrowserHeaders.AppendValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default JsArray<String> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface BrowserHeadersOptionsType {
        @JsOverlay
        static BrowserHeaders.BrowserHeadersOptionsType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isSplitValues();

        @JsProperty
        void setSplitValues(boolean splitValues);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConstructorInitJsPropertyMapTypeParameterUnionType {
        @JsOverlay
        static BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default JsArray<String> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConstructorInitUnionType {
        @JsOverlay
        static BrowserHeaders.ConstructorInitUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsPropertyMap<BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType> asJsPropertyMap() {
            return Js.cast(this);
        }

        @JsOverlay
        default Map<String, BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType> asMap() {
            return Js.cast(this);
        }

        @JsOverlay
        default Object asObject() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isObject() {
            return (Object) this instanceof Object;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsFunction
    public interface ForEachCallbackFn {
        void onInvoke(String p0, JsArray<String> p1);

        @JsOverlay
        default void onInvoke(String p0, String[] p1) {
            onInvoke(p0, Js.<JsArray<String>>uncheckedCast(p1));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetValueUnionType {
        @JsOverlay
        static BrowserHeaders.SetValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default JsArray<String> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    public JsPropertyMap<JsArray<String>> headersMap;

    public BrowserHeaders() {}

    public BrowserHeaders(BrowserHeaders init, BrowserHeaders.BrowserHeadersOptionsType options) {}

    public BrowserHeaders(BrowserHeaders init) {}

    public BrowserHeaders(
            BrowserHeaders.ConstructorInitUnionType init,
            BrowserHeaders.BrowserHeadersOptionsType options) {}

    public BrowserHeaders(BrowserHeaders.ConstructorInitUnionType init) {}

    public BrowserHeaders(
            JsPropertyMap<BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType> init,
            BrowserHeaders.BrowserHeadersOptionsType options) {}

    public BrowserHeaders(
            JsPropertyMap<BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType> init) {}

    public BrowserHeaders(
            Map<String, BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType> init,
            BrowserHeaders.BrowserHeadersOptionsType options) {}

    public BrowserHeaders(
            Map<String, BrowserHeaders.ConstructorInitJsPropertyMapTypeParameterUnionType> init) {}

    public BrowserHeaders(Object init, BrowserHeaders.BrowserHeadersOptionsType options) {}

    public BrowserHeaders(Object init) {}

    public BrowserHeaders(String init, BrowserHeaders.BrowserHeadersOptionsType options) {}

    public BrowserHeaders(String init) {}

    public native void append(String key, BrowserHeaders.AppendValueUnionType value);

    @JsOverlay
    public final void append(String key, JsArray<String> value) {
        append(key, Js.<BrowserHeaders.AppendValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void append(String key, String value) {
        append(key, Js.<BrowserHeaders.AppendValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void append(String key, String[] value) {
        append(key, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void appendFromString(String str);

    public native void delete(String key, String value);

    public native void delete(String key);

    public native void forEach(BrowserHeaders.ForEachCallbackFn callback);

    public native JsArray<String> get(String key);

    public native boolean has(String key, String value);

    public native boolean has(String key);

    @JsOverlay
    public final void set(String key, JsArray<String> value) {
        set(key, Js.<BrowserHeaders.SetValueUnionType>uncheckedCast(value));
    }

    public native void set(String key, BrowserHeaders.SetValueUnionType value);

    @JsOverlay
    public final void set(String key, String value) {
        set(key, Js.<BrowserHeaders.SetValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void set(String key, String[] value) {
        set(key, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native Object toHeaders();
}
