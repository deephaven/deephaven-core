package io.deephaven.javascript.proto.dhinternal.grpcweb;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.detach", namespace = JsPackage.GLOBAL)
public class Detach {
    @JsFunction
    public interface DetachCbFn {
        void onInvoke();
    }

    public static native void detach(Detach.DetachCbFn cb);
}
