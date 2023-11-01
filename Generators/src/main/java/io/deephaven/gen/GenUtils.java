package io.deephaven.gen;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.LinkedHashSet;
import java.util.Set;

public class GenUtils {
    private GenUtils() {
    }

    public static Set<String> typesToImport(Type t) {
        Set<String> result = new LinkedHashSet<>();

        if (t instanceof Class) {
            final Class<?> c = (Class) t;
            final boolean isArray = c.isArray();
            final boolean isPrimitive = c.isPrimitive();

            if (isPrimitive) {
                return result;
            } else if (isArray) {
                return typesToImport(c.getComponentType());
            } else {
                result.add(t.getTypeName());
            }
        } else if (t instanceof ParameterizedType) {
            final ParameterizedType pt = (ParameterizedType) t;
            result.add(pt.getRawType().getTypeName());

            for (Type a : pt.getActualTypeArguments()) {
                result.addAll(typesToImport(a));
            }
        } else if (t instanceof TypeVariable) {
            // type variables are generic so they don't need importing
            return result;
        } else if (t instanceof GenericArrayType) {
            GenericArrayType at = (GenericArrayType) t;
            return typesToImport(at.getGenericComponentType());
        } else {
            throw new UnsupportedOperationException("Unsupported Type type: " + t.getClass());
        }

        return result;
    }
}
