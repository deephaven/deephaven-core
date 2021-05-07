package io.deephaven.web.client.fu;

import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import jsinterop.annotations.JsMethod;

public class JsPromise {

    private JsPromise() {}

    @JsMethod(namespace = "Promise", name = "all")
    public static native <V> Promise<V[]> all(IThenable<? extends V>[] promises);

    @JsMethod(namespace = "Promise", name = "race")
    public static native <V> Promise<V> race(IThenable<? extends V>[] promises);
}
