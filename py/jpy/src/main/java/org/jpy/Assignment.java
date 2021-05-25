package org.jpy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class Assignment {

    private static final Map<Class<?>, Class<?>> boxedToPrimitive;

    static {
        boxedToPrimitive = new HashMap<>();
        boxedToPrimitive.put(Boolean.class, boolean.class);
        boxedToPrimitive.put(Byte.class, byte.class);
        boxedToPrimitive.put(Character.class, char.class);
        boxedToPrimitive.put(Short.class, short.class);
        boxedToPrimitive.put(Integer.class, int.class);
        boxedToPrimitive.put(Long.class, long.class);
        boxedToPrimitive.put(Float.class, float.class);
        boxedToPrimitive.put(Double.class, double.class);
    }

    static Optional<Class<?>> getUnboxedType(Class<?> clazz) {
        return Optional.ofNullable(boxedToPrimitive.get(clazz));
    }

    static boolean isAssignableFrom(Class<?> signatureType, Object instance) {
        return isAssignableFrom(signatureType, instance.getClass());
    }

    static boolean isAssignableFrom(Class<?> signatureType, Class<?> instanceType) {
        if (signatureType.isAssignableFrom(instanceType)) {
            return true;
        }
        if (signatureType.isPrimitive()) {
            return getUnboxedType(instanceType)
                .filter(signatureType::isAssignableFrom)
                .isPresent();
        }
        return false;
    }
}
