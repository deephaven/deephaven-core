package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.jspb.map.Iterator;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsConstructorFn;

@JsType(isNative = true, name = "dhinternal.jspb.Map", namespace = JsPackage.GLOBAL)
public class Map<K, V> {
    @JsFunction
    public interface ForEachCallbackFn<K, V> {
        void onInvoke(V p0, K p1);
    }

    @JsFunction
    public interface ToObjectValueToObjectFn<VO, V> {
        VO onInvoke(boolean p0, V p1);
    }

    public static native <TK, TV> Map<TK, TV> fromObject(
        JsArray<JsArray<Object>> entries, Object valueCtor, Object valueFromObject);

    @JsOverlay
    public static final <TK, TV> Map<TK, TV> fromObject(
        Object[][] entries, Object valueCtor, Object valueFromObject) {
        return fromObject(
            Js.<JsArray<JsArray<Object>>>uncheckedCast(entries), valueCtor, valueFromObject);
    }

    public Map(JsArray<JsArray<Object>> arr, JsConstructorFn<? extends V> valueCtor) {}

    public Map(JsArray<JsArray<Object>> arr) {}

    public Map(Object[][] arr, JsConstructorFn<? extends V> valueCtor) {}

    public Map(Object[][] arr) {}

    public native void clear();

    public native boolean del(K key);

    public native Iterator<JsArray<Object>> entries();

    public native void forEach(Map.ForEachCallbackFn<? super K, ? super V> callback,
        Object thisArg);

    public native void forEach(Map.ForEachCallbackFn<? super K, ? super V> callback);

    public native V get(K key);

    public native JsArray<JsArray<Object>> getEntryList();

    public native double getLength();

    public native boolean has(K key);

    public native Iterator<K> keys();

    public native Map<K, V> set(K key, V value);

    public native JsArray<JsArray<Object>> toArray();

    public native JsArray<JsArray<Object>> toObject();

    public native <VO> JsArray<JsArray<Object>> toObject(
        boolean includeInstance,
        Map.ToObjectValueToObjectFn<? extends VO, ? super V> valueToObject);

    public native JsArray<JsArray<Object>> toObject(boolean includeInstance);
}
