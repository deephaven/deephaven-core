/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.apache.commons.lang3.ClassUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ClassUtil {

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
     * Finds and caches Class instances based on name. This implementation can handle the strings created by
     * {@link Class#getName()} and {@link Class#getCanonicalName()}, and some mixture of the two. JNI names are not
     * supported.
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

    private static class FailedToResolve {
    }
}
