/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

public class ClassUtil {
    private static final Map<String, Class<?>> classMap = new HashMap<>();
    public static Map<String, Class> primitives = new HashMap<>();

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

    private static Class getJavaType(String selectedType) throws ClassNotFoundException {
        int arrayCount = 0;
        while (selectedType.endsWith("[]")) {
            selectedType = selectedType.substring(0, selectedType.length() - 2);
            arrayCount++;
        }
        Class result = primitives.get(selectedType);
        if (result == null && selectedType.startsWith("java.lang.")) {
            result = primitives.get(selectedType.substring("java.lang.".length()));
        }
        if (result == null) {
            result = Class.forName(selectedType.split("<")[0]);
        }
        if (arrayCount > 0 && result != null) {
            int dimensions[] = new int[arrayCount];
            result = Array.newInstance(result, dimensions).getClass();
        }
        return result;
    }

    public static Class<?> lookupClass(String name) throws ClassNotFoundException {
        Class result = classMap.get(name);
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
