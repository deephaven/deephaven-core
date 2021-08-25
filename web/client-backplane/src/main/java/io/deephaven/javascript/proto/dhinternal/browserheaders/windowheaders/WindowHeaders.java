package io.deephaven.javascript.proto.dhinternal.browserheaders.windowheaders;

import elemental2.core.JsArray;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.browserHeaders.WindowHeaders.WindowHeaders",
        namespace = JsPackage.GLOBAL)
public interface WindowHeaders {
    @JsFunction
    public interface ForEachCallbackFn {
        void onInvoke(String p0, String p1);
    }

    void append(String key, String value);

    void delete(String key);

    Object entries();

    Object forEach(WindowHeaders.ForEachCallbackFn callback);

    JsArray<String> get(String key);

    JsArray<String> getAll(String key);

    boolean has(String key);

    Object keys();

    void set(String key, String value);
}
