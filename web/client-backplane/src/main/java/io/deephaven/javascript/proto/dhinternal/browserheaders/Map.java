package io.deephaven.javascript.proto.dhinternal.browserheaders;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.browserHeaders.Map", namespace = JsPackage.GLOBAL)
public interface Map<K, V> {
    @JsFunction
    public interface ForEachCallbackFn<K, V> {
        void onInvoke(V p0, K p1, Map p2);
    }

    void clear();

    boolean delete(K key);

    void forEach(Map.ForEachCallbackFn<? super K, ? super V> callbackfn, Object thisArg);

    void forEach(Map.ForEachCallbackFn<? super K, ? super V> callbackfn);

    V get(K key);

    @JsProperty
    double getSize();

    boolean has(K key);

    Map<K, V> set(K key, V value);

    Map<K, V> set(K key);

    @JsProperty
    void setSize(double size);
}
