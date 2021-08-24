package io.deephaven.util.type;

import org.jetbrains.annotations.NotNull;

public class ClassUtils {
    /**
     * Gets the specified className, and if it is assignable from the expectedType returns it.
     * Otherwise throws a RuntimeException.
     *
     * @param className the class we would like to retrieve
     * @param expectedType the type of class we expect className to be
     * @param <T> expectedType
     * @return the Class object for className
     */
    @NotNull
    static public <T> Class<? extends T> checkedClassForName(final String className,
        final Class<T> expectedType) {
        Class<?> resultClass;
        try {
            resultClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not find class: " + className, e);
        }

        if (!expectedType.isAssignableFrom(resultClass)) {
            throw new RuntimeException("Invalid session class, " + resultClass.getCanonicalName()
                + ", does not implement " + expectedType.getCanonicalName());
        }

        // noinspection unchecked
        return (Class<? extends T>) resultClass;
    }
}
