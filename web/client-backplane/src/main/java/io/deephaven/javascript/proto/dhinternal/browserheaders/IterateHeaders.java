package io.deephaven.javascript.proto.dhinternal.browserheaders;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.browserheaders.windowheaders.WindowHeaders;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
        isNative = true,
        name = "dhinternal.browserHeaders.iterateHeaders",
        namespace = JsPackage.GLOBAL)
public class IterateHeaders {
    @JsFunction
    public interface IterateHeadersCallbackFn {
        void onInvoke(JsArray<String> p0);

        @JsOverlay
        default void onInvoke(String[] p0) {
            onInvoke(Js.<JsArray<String>>uncheckedCast(p0));
        }
    }

    @JsFunction
    public interface IterateHeadersKeysCallbackFn {
        void onInvoke(String p0);
    }

    public static native void iterateHeaders(
            WindowHeaders headers, IterateHeaders.IterateHeadersCallbackFn callback);

    public static native void iterateHeadersKeys(
            WindowHeaders headers, IterateHeaders.IterateHeadersKeysCallbackFn callback);
}
