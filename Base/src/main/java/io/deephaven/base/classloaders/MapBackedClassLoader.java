/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.classloaders;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * @note This ClassLoader doesn't follow the standard delegation model - it tries to find the class itself first.
 */
public class MapBackedClassLoader extends ClassLoader {

    private final Map<String, byte[]> classData = new HashMap<>();

    public synchronized void addClassData(@NotNull final String name, @NotNull final byte[] data) {
        classData.put(name, data);
    }

    @Override
    protected synchronized Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        Class<?> clazz = findLoadedClass(name);
        if (clazz == null) {
            try {
                clazz = findClass(name);
            } catch (ClassNotFoundException e) {
                clazz = getParent().loadClass(name);
            }
        }
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    protected synchronized Class<?> findClass(final String name) throws ClassNotFoundException {
        final byte[] clazzData = classData.get(name);
        if (clazzData != null) {
            return defineClass(name, clazzData, 0, clazzData.length);
        }
        throw new ClassNotFoundException();
    }
}
