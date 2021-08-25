package io.deephaven.db.v2.sources.chunk.util;

import java.util.HashMap;

public final class SimpleTypeMap<V> {

    public static <V> SimpleTypeMap<V> create(V forBoolean, V forChar, V forByte, V forShort, V forInt, V forLong,
            V forFloat, V forDouble, V forObject) {
        final HashMap<Class<?>, V> map = new HashMap<>();
        map.put(boolean.class, forBoolean);
        map.put(char.class, forChar);
        map.put(byte.class, forByte);
        map.put(short.class, forShort);
        map.put(int.class, forInt);
        map.put(long.class, forLong);
        map.put(float.class, forFloat);
        map.put(double.class, forDouble);
        return new SimpleTypeMap<>(map, forObject);
    }

    private final HashMap<Class<?>, V> map;
    private final V defaultValue;

    private SimpleTypeMap(HashMap<Class<?>, V> map, V defaultValue) {
        this.map = map;
        this.defaultValue = defaultValue;
    }

    public final V get(Class<?> simpleType) {
        return map.getOrDefault(simpleType, defaultValue);
    }
}
