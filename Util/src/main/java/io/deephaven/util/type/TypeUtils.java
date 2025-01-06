//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.type;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.util.QueryConstants.*;

/**
 * Utility functions to convert primitive types.
 */
@SuppressWarnings("unused")
public class TypeUtils {
    private static final Map<Class<?>, Class<?>> primitiveToBoxed;
    private static final Map<Class<?>, Class<?>> boxedToPrimitive;

    public static final Set<Class<?>> PRIMITIVE_TYPES;
    public static final Set<Class<?>> BOXED_TYPES;

    public static final Map<String, Class<?>> primitiveClassNameToClass;

    static {
        LinkedHashMap<Class<?>, Class<?>> primitiveToBoxedTemp = new LinkedHashMap<>();
        LinkedHashMap<Class<?>, Class<?>> boxedToPrimitiveTemp = new LinkedHashMap<>();

        // Note: ordering here matters! Tests in TestLanguageParser depend on it.
        primitiveToBoxedTemp.put(byte.class, Byte.class);
        primitiveToBoxedTemp.put(short.class, Short.class);
        primitiveToBoxedTemp.put(char.class, Character.class);
        primitiveToBoxedTemp.put(int.class, Integer.class);
        primitiveToBoxedTemp.put(long.class, Long.class);
        primitiveToBoxedTemp.put(float.class, Float.class);
        primitiveToBoxedTemp.put(double.class, Double.class);
        primitiveToBoxedTemp.put(boolean.class, Boolean.class);
        for (Map.Entry<Class<?>, Class<?>> classClassEntry : primitiveToBoxedTemp.entrySet()) {
            boxedToPrimitiveTemp.put(classClassEntry.getValue(), classClassEntry.getKey());
        }

        primitiveToBoxed = Collections.unmodifiableMap(primitiveToBoxedTemp);
        boxedToPrimitive = Collections.unmodifiableMap(boxedToPrimitiveTemp);

        PRIMITIVE_TYPES = Collections.unmodifiableSet(primitiveToBoxedTemp.keySet());
        BOXED_TYPES = Collections.unmodifiableSet(new LinkedHashSet<>(primitiveToBoxedTemp.values()));
        primitiveClassNameToClass = Collections
                .unmodifiableMap(PRIMITIVE_TYPES.stream().collect(Collectors.toMap(Class::getName, type -> type)));
    }

    /**
     * Deprecated with no replacement.
     */
    @Deprecated
    @Retention(RetentionPolicy.RUNTIME)
    public @interface IsDateTime {
        boolean value() default true;
    }

    /**
     * Returns a reference type corresponding to the given {@code type}. If {@code type} is itself a reference type,
     * then {@code type} is returned. If {@code type} is a primitive type, then the appropriate boxed type is returned.
     *
     * @param type The type
     */
    public static Class<?> getBoxedType(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        return primitiveToBoxed.get(type);
    }

    /**
     * Returns the primitive type corresponding to the given {@code type}. If {@code type} is itself a primitive type,
     * then {@code type} is returned. If {@code type} is neither a primitive type nor a boxed type, then {@code null} is
     * returned.
     *
     * @param type The type
     * @return type's primitive equivalent, or null
     */
    public static Class<?> getUnboxedType(Class<?> type) {
        if (type.isPrimitive()) {
            return type;
        }
        return boxedToPrimitive.get(type);
    }

    /**
     * Same as {@link #getUnboxedType(Class)}, but returns non-wrapper classes unmolested.
     *
     * @param type The type
     * @return type's unboxed equivalent, or type
     */
    public static Class<?> getUnboxedTypeIfBoxed(@NotNull final Class<?> type) {
        final Class<?> unboxedType = getUnboxedType(type);
        return unboxedType != null ? unboxedType : type;
    }

    public static byte[] toByteArray(float[] array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    public static byte[] toByteArray(int[] array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    public static byte[] toByteArray(short[] array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    public static byte[] toByteArray(long[] array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    public static byte[] toByteArray(double[] array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    public static float[] toFloatArray(byte[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static float[] toFloatArray(int[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (float) array[i];
        }
        return result;
    }

    public static float[] toFloatArray(short[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static float[] toFloatArray(long[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (float) array[i];
        }
        return result;
    }

    public static float[] toFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (float) array[i];
        }
        return result;
    }

    public static short[] toShortArray(float[] array) {
        short[] result = new short[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (short) array[i];
        }
        return result;
    }

    public static short[] toShortArray(int[] array) {
        short[] result = new short[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (short) array[i];
        }
        return result;
    }

    public static short[] toShortArray(byte[] array) {
        short[] result = new short[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static short[] toShortArray(long[] array) {
        short[] result = new short[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (short) array[i];
        }
        return result;
    }

    public static short[] toShortArray(double[] array) {
        short[] result = new short[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (short) array[i];
        }
        return result;
    }

    public static long[] toLongArray(float[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (long) array[i];
        }
        return result;
    }

    public static long[] toLongArray(int[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static long[] toLongArray(short[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static long[] toLongArray(byte[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static long[] toLongArray(double[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (long) array[i];
        }
        return result;
    }

    public static int[] toIntArray(float[] array) {
        int[] result = new int[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (int) array[i];
        }
        return result;
    }

    public static int[] toIntArray(byte[] array) {
        int[] result = new int[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static int[] toIntArray(short[] array) {
        int[] result = new int[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static int[] toIntArray(long[] array) {
        int[] result = new int[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (int) array[i];
        }
        return result;
    }

    public static int[] toIntArray(double[] array) {
        int[] result = new int[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (int) array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(float[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(int[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(short[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(long[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (double) array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(double[] array) {
        double[] result = new double[array.length];
        System.arraycopy(array, 0, result, 0, result.length);
        return result;
    }

    public static boolean isConvertibleToPrimitive(Class<?> type) {
        final Class<?> unboxedType = TypeUtils.getUnboxedType(type);
        return unboxedType != null && unboxedType != boolean.class; // TODO: isConvertibleToPrimitive(Boolean.class) ==
                                                                    // false ???
    }

    public static boolean isBoxedType(Class<?> exprType) {
        return BOXED_TYPES.contains(exprType);
    }

    public static String nullConstantForType(Class<?> type) {
        if (type == char.class) {
            return "QueryConstants.NULL_CHAR";
        } else if (type == byte.class) {
            return "QueryConstants.NULL_BYTE";
        } else if (type == short.class) {
            return "QueryConstants.NULL_SHORT";
        } else if (type == int.class) {
            return "QueryConstants.NULL_INT";
        } else if (type == long.class) {
            return "QueryConstants.NULL_LONG";
        } else if (type == Boolean.class) {
            return "QueryConstants.NULL_BOOLEAN";
        } else if (type == double.class) {
            return "QueryConstants.NULL_DOUBLE";
        } else if (type == float.class) {
            return "QueryConstants.NULL_FLOAT";
        } else {
            return "null";
        }
    }

    /**
     * Whether the class is equal to char.class.
     *
     * @param c class
     * @return true if {@code c} equals char.class, false otherwise
     */
    public static boolean isPrimitiveChar(@NotNull final Class<?> c) {
        return c == char.class;
    }

    /**
     * Whether the class is an instance of Character.class.
     *
     * @param c class
     * @return true if Character.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedChar(@NotNull final Class<?> c) {
        return Character.class == c;
    }

    /**
     * Whether the class is an instance of Integer.class.
     *
     * @param c class
     * @return true if Integer.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedInteger(@NotNull final Class<?> c) {
        return Integer.class == c;
    }

    /**
     * Whether the class is an instance of Long.class.
     *
     * @param c class
     * @return true if Long.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedLong(@NotNull final Class<?> c) {
        return Long.class == c;
    }

    /**
     * Whether the class is an instance of Short.class.
     *
     * @param c class
     * @return true if Short.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedShort(@NotNull final Class<?> c) {
        return Short.class == c;
    }

    /**
     * Whether the class is an instance of Float.class.
     *
     * @param c class
     * @return true if Float.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedFloat(@NotNull final Class<?> c) {
        return Float.class == c;
    }

    /**
     * Whether the class is an instance of Double.class.
     *
     * @param c class
     * @return true if Double.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedDouble(@NotNull final Class<?> c) {
        return Double.class == c;
    }

    /**
     * Whether the class is an instance of Byte.class.
     *
     * @param c class
     * @return true if Byte.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedByte(@NotNull final Class<?> c) {
        return Byte.class == c;
    }

    /**
     * Whether the class is a boxed arithmetic type (Long, Integer, Short, Byte)
     *
     * @param c class
     * @return true if the class is a boxed arithmetic type, false otherwise
     */
    public static boolean isBoxedArithmetic(@NotNull final Class<?> c) {
        return isBoxedLong(c) || isBoxedInteger(c) || isBoxedShort(c) || isBoxedByte(c);
    }

    /**
     * Whether the class is an instance of Boolean.class.
     *
     * @param c class
     * @return true if Boolean.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedBoolean(@NotNull final Class<?> c) {
        return Boolean.class == c;
    }

    /**
     * Whether the class equals char.class or Character.class is assignable from it.
     *
     * @param c class
     * @return true if Character.class is assignable from {@code c} or {@code c} equals char.class
     */
    public static boolean isCharacter(@NotNull final Class<?> c) {
        return isPrimitiveChar(c) || isBoxedChar(c);
    }

    /**
     * Whether the class is a {@link String}
     *
     * @param type the class
     * @return true if the type is a String, false otherwise
     */
    public static boolean isString(Class<?> type) {
        return String.class == type;
    }

    /**
     * Checks if the type is a primitive or Boxed floate type (double or float).
     *
     * @param type the class
     * @return true if it is a float type, false otherwise
     */
    public static boolean isFloatType(Class<?> type) {
        return type.equals(double.class) || type.equals(float.class) || isBoxedDouble(type) || isBoxedFloat(type);
    }

    public static abstract class TypeBoxer<T> {
        public abstract T get(T result);
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeBoxer<T> getTypeBoxer(Class<T> type) {
        if (type == byte.class || type == Byte.class) {
            return (TypeBoxer<T>) new TypeBoxer<Byte>() {
                @Override
                public Byte get(Byte result) {
                    return (result == NULL_BYTE ? null : result);
                }
            };
        } else if (type == char.class || type == Character.class) {
            return (TypeBoxer<T>) new TypeBoxer<Character>() {
                @Override
                public Character get(Character result) {
                    return (result == NULL_CHAR ? null : result);
                }
            };
        } else if (type == double.class || type == Double.class) {
            return (TypeBoxer<T>) new TypeBoxer<Double>() {
                @Override
                public Double get(Double result) {
                    return (result == NULL_DOUBLE ? null : result);
                }
            };
        } else if (type == float.class || type == Float.class) {
            return (TypeBoxer<T>) new TypeBoxer<Float>() {
                @Override
                public Float get(Float result) {
                    return (result == NULL_FLOAT ? null : result);
                }
            };
        } else if (type == int.class || type == Integer.class) {
            return (TypeBoxer<T>) new TypeBoxer<Integer>() {
                @Override
                public Integer get(Integer result) {
                    return (result == NULL_INT ? null : result);
                }
            };
        } else if (type == long.class || type == Long.class) {
            return (TypeBoxer<T>) new TypeBoxer<Long>() {
                @Override
                public Long get(Long result) {
                    return (result == NULL_LONG ? null : result);
                }
            };
        } else if (type == short.class || type == Short.class) {
            return (TypeBoxer<T>) new TypeBoxer<Short>() {
                @Override
                public Short get(Short result) {
                    return (result == NULL_SHORT ? null : result);
                }
            };
        } else {
            return new TypeBoxer<>() {
                @Override
                public Object get(Object result) {
                    return result;
                }
            };
        }
    }

    public static Boolean box(Boolean value) {
        return value;
    }

    public static Byte box(byte value) {
        return value == NULL_BYTE ? null : value;
    }

    public static Character box(char value) {
        return value == NULL_CHAR ? null : value;
    }

    public static Double box(double value) {
        return value == NULL_DOUBLE ? null : value;
    }

    public static Float box(float value) {
        return value == NULL_FLOAT ? null : value;
    }

    public static Integer box(int value) {
        return value == NULL_INT ? null : value;
    }

    public static Long box(long value) {
        return value == NULL_LONG ? null : value;
    }

    public static Short box(short value) {
        return value == NULL_SHORT ? null : value;
    }

    public static boolean unbox(Boolean value) {
        // This will throw an NPE on a null value.
        return value;
    }

    public static byte unbox(Byte value) {
        return (value == null ? NULL_BYTE : value);
    }

    public static char unbox(Character value) {
        return (value == null ? NULL_CHAR : value);
    }

    public static double unbox(Double value) {
        return (value == null ? NULL_DOUBLE : value);
    }

    public static float unbox(Float value) {
        return (value == null ? NULL_FLOAT : value);
    }

    public static int unbox(Integer value) {
        return (value == null ? NULL_INT : value);
    }

    public static long unbox(Long value) {
        return (value == null ? NULL_LONG : value);
    }

    public static short unbox(Short value) {
        return (value == null ? NULL_SHORT : value);
    }
}
