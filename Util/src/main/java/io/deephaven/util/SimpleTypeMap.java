//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import java.util.HashMap;

public final class SimpleTypeMap<V> {

    /**
     * Create a mapping from type {@link Class classes} to a value.
     *
     * @param forBoolean The mapping for {@code boolean} types (<em>note</em> {@link Boolean} maps to {@code forObject})
     * @param forChar The mapping for {@code char} and {@link Character} types
     * @param forByte The mapping for {@code byte} and {@link Byte} types
     * @param forShort The mapping for {@code short} and {@link Short} types
     * @param forInt The mapping for {@code int} and {@link Integer} types
     * @param forLong The mapping for {@code long} and {@link Long} types
     * @param forFloat The mapping for {@code float} and {@link Float} types
     * @param forDouble The mapping for {@code double} and {@link Double} types
     * @param forObject The mapping for all other types
     * @return A SimpleTypeMap to the provided values
     */
    public static <V> SimpleTypeMap<V> create(
            V forBoolean,
            V forChar,
            V forByte,
            V forShort,
            V forInt,
            V forLong,
            V forFloat,
            V forDouble,
            V forObject) {
        final HashMap<Class<?>, V> map = new HashMap<>();

        map.put(boolean.class, forBoolean);
        // Note: Booleans are treated as Objects, unlike other boxed primitives

        map.put(char.class, forChar);
        map.put(Character.class, forChar);

        map.put(byte.class, forByte);
        map.put(Byte.class, forByte);

        map.put(short.class, forShort);
        map.put(Short.class, forShort);

        map.put(int.class, forInt);
        map.put(Integer.class, forInt);

        map.put(long.class, forLong);
        map.put(Long.class, forLong);

        map.put(float.class, forFloat);
        map.put(Float.class, forFloat);

        map.put(double.class, forDouble);
        map.put(Double.class, forDouble);

        return new SimpleTypeMap<>(map, forObject);
    }

    private final HashMap<Class<?>, V> map;
    private final V defaultValue;

    private SimpleTypeMap(HashMap<Class<?>, V> map, V defaultValue) {
        this.map = map;
        this.defaultValue = defaultValue;
    }

    public V get(Class<?> simpleType) {
        return map.getOrDefault(simpleType, defaultValue);
    }
}
