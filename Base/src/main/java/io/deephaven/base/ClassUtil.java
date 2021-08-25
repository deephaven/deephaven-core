/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public final class ClassUtil {
    public static String getBaseName(final String s) {
        int i = s.lastIndexOf(".");
        return (i == -1) ? s : s.substring(i + 1);
    }

    public static void dumpFinals(final Logger log, final String prefix, final Object p) {
        final Class c = p.getClass();
        Field[] fields = c.getDeclaredFields();
        final int desiredMods = (Modifier.PUBLIC | Modifier.FINAL);
        for (Field f : fields) {
            if ((f.getModifiers() & desiredMods) == 0)
                continue;
            try {
                final String tName = f.getType().getName();
                final String name = f.getName();
                final Object value = f.get(p);
                log.info(prefix
                    + tName
                    + " " + name
                    + " = " + value.toString());
            } catch (Exception ignored) {
            }
        }
    }

    public static <T> Class<T> generify(Class c) {
        return c;
    }

    private static final Map<String, Class<?>> classMap = new HashMap<>();
    public static Map<String, Class<?>> primitives = new HashMap<>();

    static {
        primitives.put("boolean", boolean.class);
        primitives.put("int", int.class);
        primitives.put("double", double.class);
        primitives.put("long", long.class);
        primitives.put("byte", byte.class);
        primitives.put("short", short.class);
        primitives.put("char", char.class);
        primitives.put("float", float.class);
    }

    private static Class<?> getJavaType(String selectedType) throws ClassNotFoundException {
        int arrayCount = 0;
        while (selectedType.endsWith("[]")) {
            selectedType = selectedType.substring(0, selectedType.length() - 2);
            ++arrayCount;
        }
        Class<?> result = primitives.get(selectedType);
        if (result == null && selectedType.startsWith("java.lang.")) {
            result = primitives.get(selectedType.substring("java.lang.".length()));
        }
        if (result == null) {
            result = Class.forName(selectedType.split("<")[0]);
        }
        if (arrayCount > 0) {
            final int[] dimensions = new int[arrayCount];
            result = Array.newInstance(result, dimensions).getClass();
        }
        return result;
    }

    public static Class<?> lookupClass(final String name) throws ClassNotFoundException {
        Class<?> result = classMap.get(name);
        if (result == null) {
            try {
                result = getJavaType(name);
                classMap.put(name, result);
            } catch (ClassNotFoundException e) {
                classMap.put(name, ClassUtil.class);
                throw e;
            }
        } else if (result == ClassUtil.class) {
            throw new ClassNotFoundException(name);
        }
        return result;
    }
}
