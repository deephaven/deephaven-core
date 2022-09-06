/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import groovy.lang.Closure;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.util.type.TypeUtils;

import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for expanding QueryScope parameter classes.
 */
public class QueryScopeParamTypeUtil {

    public static Class<?> classFromType(final Type declaredType) {
        if (declaredType instanceof Class<?>) {
            return (Class<?>) declaredType;
        }
        if (declaredType instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) declaredType).getRawType();
        }
        return null;
    }

    /**
     * Get a map from binary name to declared type for the dynamic classes referenced by an array of param classes.
     *
     * @param params The parameters to operate on
     * @return The result map
     */
    public static Map<String, Class<?>> expandParameterClasses(final List<Class<?>> params) {
        final Map<String, Class<?>> found = new HashMap<>();
        params.forEach(cls -> visitParameterClass(found, cls));
        return found;
    }

    private static void visitParameterClass(final Map<String, Class<?>> found, Class<?> cls) {
        while (cls.isArray()) {
            cls = classFromType(cls.getComponentType());
        }

        final String name = cls.getName();
        if (!name.startsWith(QueryCompiler.DYNAMIC_GROOVY_CLASS_PREFIX)) {
            return;
        }

        final Class<?> seen = found.get(name);
        if (seen != null) {
            if (seen != cls) {
                throw new UnsupportedOperationException(
                        "Parameter list may not include multiple versions of the same class: "
                                + name + ". Was the class redefined in your shell?");
            }
            // we don't need to revisit this class
            return;
        }
        found.put(name, cls);

        // Visit Methods
        Arrays.stream(cls.getMethods()).forEach(m -> {
            visitParameterClass(found, m.getReturnType());
            Arrays.stream(m.getParameterTypes()).forEach(t -> visitParameterClass(found, t));
        });
        // Visit Fields
        Arrays.stream(cls.getFields()).forEach(f -> visitParameterClass(found, f.getType()));
        final Class<?> componentType = cls.getComponentType();
        if (componentType != null) {
            visitParameterClass(found, componentType);
        }
    }

    public static <T> Class<?> getDeclaredClass(final T value) {
        Type declaredType = getDeclaredType(value);
        Class<?> cls = classFromType(declaredType);

        if (cls == null) {
            throw new IllegalStateException("Unexpected declared type of type '"
                    + declaredType.getClass().getCanonicalName() + "'");
        }

        return cls;
    }

    public static <T> Type getDeclaredType(final T value) {
        // in newer versions of groovy, our closures will be subtypes that evade the logic in getDeclaredType
        // (they will return a null Class#getCanonicalName b/c they are dynamic classes).
        final Class<?> type;
        if (value == null) {
            type = Object.class;
        } else if (value instanceof Enum) {
            type = ((Enum<?>) value).getDeclaringClass();
        } else if (value instanceof Closure) {
            type = Closure.class;
        } else {
            type = value.getClass();
        }
        return getDeclaredType(type);
    }

    private static Type getDeclaredType(final Class<?> origType) {
        Class<?> type = origType;
        while (type != Object.class) {
            if (Modifier.isPublic(type.getModifiers()) && !type.isAnonymousClass()) {
                break;
            }

            Type[] interfaces = type.getGenericInterfaces();
            for (Type ityp : interfaces) {
                Class<?> iface = null;
                if (ityp instanceof Class<?>) {
                    iface = (Class<?>) ityp;
                } else if (ityp instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) ityp;
                    Type rawType = pt.getRawType();
                    if (rawType instanceof Class<?>) {
                        iface = (Class<?>) rawType;
                    }
                }

                if (iface != null && Modifier.isPublic(iface.getModifiers()) && iface.getMethods().length > 0) {
                    return ityp;
                }
            }

            type = type.getSuperclass();
        }

        return type;
    }

    public static <T> String getDeclaredTypeName(T value) {
        return getDeclaredClass(value).getCanonicalName();
    }

    public static <T> String getPrimitiveTypeNameIfAvailable(final T value) {
        if (value == null) {
            return getDeclaredTypeName(null);
        }
        Class<?> type = getDeclaredClass(value);
        if (TypeUtils.isBoxedType(type)) {
            return TypeUtils.getUnboxedType(type).getCanonicalName();
        }
        return getDeclaredTypeName(value);
    }
}
