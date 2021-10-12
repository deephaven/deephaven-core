/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.apache.commons.lang3.ClassUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    private static final Map<String, Class<?>> classMap = new ConcurrentHashMap<>();

    private static Class<?> getJavaType(String selectedType) throws ClassNotFoundException {
        // Given string might have generics, remove those before delegating to
        // commons-lang3 for lookup implementation. Greedily match from first
        // '<' to last '>' and remove all, so that the two types of array
        // notation are retained.
        String noGenerics = selectedType.replaceAll("<.*>", "");

        return ClassUtils.getClass(noGenerics, false);
    }

    /**
     * Finds and caches Class instances based on name. This implementation can handle the strings
     * created by {@link Class#getName()} and {@link Class#getCanonicalName()}, and some mixture
     * of the two. JNI names are not supported.
     *
     * @param name the name of the class to lookup.
     * @return A class instance
     * @throws ClassNotFoundException if the class cannot be found
     */
    public static Class<?> lookupClass(final String name) throws ClassNotFoundException {
        Class<?> result = classMap.get(name);
        if (result == null) {
            try {
                result = getJavaType(name);
                classMap.put(name, result);
            } catch (ClassNotFoundException e) {
                // Note that this prevents some runtime fix to the classpath and retrying
                classMap.put(name, FailedToResolve.class);
                throw e;
            }
        } else if (result == FailedToResolve.class) {
            throw new ClassNotFoundException(name);
        }
        return result;
    }

    private static class FailedToResolve {}
}
