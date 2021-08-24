/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.type;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import java.lang.reflect.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * Utility functions to convert primitive types.
 */
@SuppressWarnings("unused")
public class TypeUtils {
    private static final Map<Class, Class> primitiveToBoxed;
    private static final Map<Class, Class> boxedToPrimitive;

    public static final Set<Class> PRIMITIVE_TYPES;
    public static final Set<Class> BOXED_TYPES;

    public static final Map<String, Class> primitiveClassNameToClass;

    static {
        LinkedHashMap<Class, Class> primitiveToBoxedTemp = new LinkedHashMap<>();
        LinkedHashMap<Class, Class> boxedToPrimitiveTemp = new LinkedHashMap<>();

        // Note: ordering here matters! Tests in TestDBLanguageParser depend on it.
        primitiveToBoxedTemp.put(byte.class, Byte.class);
        primitiveToBoxedTemp.put(short.class, Short.class);
        primitiveToBoxedTemp.put(char.class, Character.class);
        primitiveToBoxedTemp.put(int.class, Integer.class);
        primitiveToBoxedTemp.put(long.class, Long.class);
        primitiveToBoxedTemp.put(float.class, Float.class);
        primitiveToBoxedTemp.put(double.class, Double.class);
        primitiveToBoxedTemp.put(boolean.class, Boolean.class);
        for (Map.Entry<Class, Class> classClassEntry : primitiveToBoxedTemp.entrySet()) {
            boxedToPrimitiveTemp.put(classClassEntry.getValue(), classClassEntry.getKey());
        }

        primitiveToBoxed = Collections.unmodifiableMap(primitiveToBoxedTemp);
        boxedToPrimitive = Collections.unmodifiableMap(boxedToPrimitiveTemp);

        PRIMITIVE_TYPES = Collections.unmodifiableSet(primitiveToBoxedTemp.keySet());
        BOXED_TYPES =
            Collections.unmodifiableSet(new LinkedHashSet<>(primitiveToBoxedTemp.values()));
        primitiveClassNameToClass = Collections.unmodifiableMap(
            PRIMITIVE_TYPES.stream().collect(Collectors.toMap(Class::getName, type -> type)));
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface IsDateTime {
        boolean value() default true;
    }

    /**
     * Returns a reference type corresponding to the given {@code type}. If {@code type} is itself a
     * reference type, then {@code type} is returned. If {@code type} is a primitive type, then the
     * appropriate boxed type is returned.
     * 
     * @param type The type
     */
    public static Class getBoxedType(Class type) {
        if (!type.isPrimitive()) {
            return type;
        }
        return primitiveToBoxed.get(type);
    }

    /**
     * Returns the primitive type corresponding to the given {@code type}. If {@code type} is itself
     * a primitive type, then {@code type} is returned. If {@code type} is neither a primitive type
     * nor a boxed type, then {@code null} is returned.
     * 
     * @param type The type
     * @return type's primitive equivalent, or null
     */
    public static Class getUnboxedType(Class type) {
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
    public static Class getUnboxedTypeIfBoxed(@NotNull final Class type) {
        final Class unboxedType = getUnboxedType(type);
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
            result[i] = (float) array[i];
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
            result[i] = (float) array[i];
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
            result[i] = (short) array[i];
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
            result[i] = (long) array[i];
        }
        return result;
    }

    public static long[] toLongArray(short[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (long) array[i];
        }
        return result;
    }

    public static long[] toLongArray(byte[] array) {
        long[] result = new long[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (long) array[i];
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
            result[i] = (int) array[i];
        }
        return result;
    }

    public static int[] toIntArray(short[] array) {
        int[] result = new int[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (int) array[i];
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
            result[i] = (double) array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(int[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (double) array[i];
        }
        return result;
    }

    public static double[] toDoubleArray(short[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (double) array[i];
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

    public static boolean isConvertibleToPrimitive(Class type) {
        final Class unboxedType = TypeUtils.getUnboxedType(type);
        return unboxedType != null && unboxedType != boolean.class; // TODO:
                                                                    // isConvertibleToPrimitive(Boolean.class)
                                                                    // == false ???
    }

    public static boolean isBoxedType(Class exprType) {
        return BOXED_TYPES.contains(exprType);
    }

    public static String nullConstantForType(Class type) {
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
     * Whether the class is equal to one of the six numeric primitives: float, double, int, long,
     * short, or byte.
     *
     * @param c class
     * @return true if {@code c} is a numeric primitive, false otherwise
     */
    public static boolean isPrimitiveNumeric(@NotNull final Class c) {
        return c.equals(double.class) || c.equals(float.class)
            || c.equals(int.class) || c.equals(long.class) || c.equals(short.class)
            || c.equals(byte.class);
    }

    /**
     * Whether the class is an instance of {@link Number}.
     *
     * @param c class
     * @return true if Number.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedNumeric(@NotNull final Class c) {
        return Number.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is equal to char.class.
     *
     * @param c class
     * @return true if {@code c} equals char.class, false otherwise
     */
    public static boolean isPrimitiveChar(@NotNull final Class c) {
        return c.equals(char.class);
    }

    /**
     * Whether the class is an instance of Character.class.
     *
     * @param c class
     * @return true if Character.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedChar(@NotNull final Class c) {
        return Character.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is an instance of Integer.class.
     *
     * @param c class
     * @return true if Integer.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedInteger(@NotNull final Class c) {
        return Integer.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is an instance of Long.class.
     *
     * @param c class
     * @return true if Long.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedLong(@NotNull final Class c) {
        return Long.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is an instance of Short.class.
     *
     * @param c class
     * @return true if Short.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedShort(@NotNull final Class c) {
        return Short.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is an instance of Float.class.
     *
     * @param c class
     * @return true if Float.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedFloat(@NotNull final Class c) {
        return Float.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is an instance of Double.class.
     *
     * @param c class
     * @return true if Double.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedDouble(@NotNull final Class c) {
        return Double.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is an instance of Byte.class.
     *
     * @param c class
     * @return true if Byte.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedByte(@NotNull final Class c) {
        return Byte.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is a boxed arithmetic type (Long, Integer, Short, Byte)
     *
     * @param c class
     * @return true if the class is a boxed arithmetic type, false otherwise
     */
    public static boolean isBoxedArithmetic(@NotNull final Class c) {
        return isBoxedLong(c) || isBoxedInteger(c) || isBoxedShort(c) || isBoxedByte(c);
    }

    /**
     * Whether the class is an instance of Boolean.class.
     *
     * @param c class
     * @return true if Boolean.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedBoolean(@NotNull final Class c) {
        return Boolean.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is {@link #isPrimitiveNumeric(Class)} or {@link #isBoxedNumeric(Class)}
     *
     * @param c class
     * @return true if {@code c} is numeric, false otherwise
     */
    public static boolean isNumeric(@NotNull final Class c) {
        return isPrimitiveNumeric(c) || isBoxedNumeric(c);
    }

    /**
     * Whether the class equals char.class or Character.class is assignable from it.
     *
     * @param c class
     * @return true if Character.class is assignable from {@code c} or {@code c} equals char.class
     */
    public static boolean isCharacter(@NotNull final Class c) {
        return isPrimitiveChar(c) || isBoxedChar(c);
    }

    /**
     * Whether the class is a DBDateTime or Date.
     *
     * @param type The class.
     * @return true if the type is a DBDateTime or {@link Date}.
     */
    public static boolean isDateTime(Class type) {
        return Date.class.isAssignableFrom(type) || type.getAnnotation(IsDateTime.class) != null
            && ((IsDateTime) type.getAnnotation(IsDateTime.class)).value();
    }

    /**
     * Whether the class is a {@link String}
     *
     * @param type the class
     * @return true if the type is a String, false otherwise
     */
    public static boolean isString(Class type) {
        return String.class.isAssignableFrom(type);
    }

    /**
     * Whether the class is a {@link BigInteger} or {@link BigDecimal}
     *
     * @param type the class
     * @return true if the type is BigInteger or BigDecimal, false otherwise
     */
    public static boolean isBigNumeric(Class type) {
        return BigInteger.class.isAssignableFrom(type) || BigDecimal.class.isAssignableFrom(type);
    }

    /**
     * Checks if a type is primitive or {@link Serializable}.
     *
     * @param type the class
     * @return true if the type is primitive or Serializable
     */
    public static boolean isPrimitiveOrSerializable(Class type) {
        return type.isPrimitive() || Serializable.class.isAssignableFrom(type);
    }

    /**
     * Checks if the type is a primitive or Boxed floate type (double or float).
     *
     * @param type the class
     * @return true if it is a float type, false otherwise
     */
    public static boolean isFloatType(Class type) {
        return type.equals(double.class) || type.equals(float.class) || isBoxedDouble(type)
            || isBoxedFloat(type);
    }

    /**
     * Converts an Object to a String for writing to a workspace. This is meant to be used in
     * conjunction with {@code TypeUtils.fromString}. Strings, Numbers, and primitives will all
     * convert using {@code Obect.toString}. Serializable objects will be encoded in base64. All
     * others will return null.
     *
     * @param o the object to convert
     * @return a String representation of the object, null if it cannot be converted
     * @throws IOException if an IO error occurs during conversion
     */
    public static String objectToString(Object o) throws IOException {
        if (o == null) {
            return null;
        }

        final Class<?> type = o.getClass();
        // isNumeric gets BigInteger and BigDecimal in addition to everything gotten by
        // isConvertibleToPrimitive
        if (type == String.class || isConvertibleToPrimitive(type) || isNumeric(type)) {
            return o.toString();
        } else if (o instanceof Serializable) {
            return encode64Serializable((Serializable) o);
        }

        throw new RuntimeException("Failed to convert object of type " + type.getCanonicalName()
            + ".  Type not supported");
    }

    /**
     * Creates an Object from a String. This is meant to be used in conjunction with
     * {@code TypeUtils.objectToString} Strings, Numbers, and primitives will all parse using their
     * boxed type parsing methods. Serializable types will be decoded from base64. Returns null if
     * the String fails to parse.
     *
     * @param string the String to parse
     * @param typeString the Canonical Name of the class type
     * @return an object parsed from the String
     * @throws RuntimeException if the string fails to parse
     * @throws IOException if an IO error occurs during conversion
     */
    public static Optional<Object> fromString(String string, String typeString) throws IOException {
        final Class<?> type;
        try {
            type = Class.forName(typeString);
            return Optional.ofNullable(fromString(string, type));
        } catch (ClassNotFoundException e) {
            return Optional.empty();
        }
    }

    /**
     * Creates an Object from a String. This is meant to be used in conjunction with
     * {@code TypeUtils.objectToString} Strings, Numbers, and primitives will all parse using their
     * boxed type parsing methods. Serializable types will be decoded from base64. Returns null if
     * the String fails to parse.
     *
     * @param string the String to parse
     * @param type the type of the object
     * @return an object parsed from the String
     * @throws RuntimeException if the string fails to parse
     * @throws IOException if an IO error occurs during conversion
     */
    public static Object fromString(String string, Class<?> type) throws IOException {
        final Class<?> boxedType = getBoxedType(type);
        try {
            if (boxedType == String.class) {
                return string;
            } else if (boxedType == Boolean.class) {
                return Boolean.parseBoolean(string);
            } else if (boxedType == Integer.class) {
                return Integer.parseInt(string);
            } else if (boxedType == Double.class) {
                return Double.parseDouble(string);
            } else if (boxedType == Short.class) {
                return Short.parseShort(string);
            } else if (boxedType == Long.class) {
                return Long.parseLong(string);
            } else if (boxedType == Float.class) {
                return Float.parseFloat(string);
            } else if (boxedType == BigInteger.class) {
                return new BigInteger(string);
            } else if (boxedType == BigDecimal.class) {
                return new BigDecimal(string);
            } else if (boxedType == Byte.class) {
                return Byte.parseByte(string);
            } else if (boxedType == Character.class) {
                return string.charAt(0);
            } else if (Serializable.class.isAssignableFrom(boxedType)) {
                return decode64Serializable(string);
            }
        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to parse " + string + "into type " + type.getCanonicalName(), e);
        }

        throw new RuntimeException("Failed to parse " + string + "into type "
            + type.getCanonicalName() + ".  Type not supported");
    }

    /**
     * Encodes a Serializable Object into base64 String.
     * 
     * @param serializable the object to encode
     * @return the base64 encoded string
     * @throws IOException if the string cannot be encoded
     */
    public static String encode64Serializable(Serializable serializable) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(serializable);
            return Base64.getEncoder().encodeToString(bos.toByteArray());
        }
    }

    /**
     * Decodes a Serializable Object from a base64 encoded String.
     *
     * @param string the base64 encoded String
     * @return the encoded Object
     * @throws IOException if the string cannot be decoded
     * @throws ClassNotFoundException if the Object type is unknown
     */
    public static Object decode64Serializable(String string)
        throws IOException, ClassNotFoundException {
        try (ObjectInputStream is =
            new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(string)))) {
            return is.readObject();
        }
    }

    /**
     * Determine the Class from the Type.
     *
     * @param paramType
     * @return
     */
    public static Class getErasedType(Type paramType) {
        if (paramType instanceof Class) {
            return (Class) paramType;
        } else if (paramType instanceof ParameterizedType) {
            return (Class) // We are asking the parameterized type for it's raw type, which is
                           // always Class
            ((ParameterizedType) paramType).getRawType();
        } else if (paramType instanceof WildcardType) {
            final Type[] upper = ((WildcardType) paramType).getUpperBounds();
            return getErasedType(upper[0]);
        } else if (paramType instanceof java.lang.reflect.TypeVariable) {
            final Type[] bounds = ((TypeVariable) paramType).getBounds();
            if (bounds.length > 1) {
                Class[] erasedBounds = new Class[bounds.length];
                Class weakest = null;
                for (int i = 0; i < erasedBounds.length; i++) {
                    erasedBounds[i] = getErasedType(bounds[i]);
                    if (i == 0) {
                        weakest = erasedBounds[i];
                    } else {
                        weakest = getWeakest(weakest, erasedBounds[i]);
                    }
                    // If we are erased to object, stop erasing...
                    if (weakest == Object.class) {
                        break;
                    }
                }
                return weakest;
            }
            return getErasedType(bounds[0]);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Determine the weakest parent of the two provided Classes.
     *
     * @param one
     * @param two
     * @return the weakest parent Class
     */
    private static Class getWeakest(Class one, Class two) {
        if (one.isAssignableFrom(two)) {
            return one;
        } else if (two.isAssignableFrom(one)) {
            return two;
        }
        // No luck on quick check... Look in interfaces.
        Set<Class<?>> oneInterfaces = getFlattenedInterfaces(one);
        Set<Class<?>> twoInterfaces = getFlattenedInterfaces(two);
        // Keep only shared interfaces
        oneInterfaces.retainAll(twoInterfaces);
        Class strongest = Object.class;
        for (Class<?> cls : oneInterfaces) {
            // There is a winning type...
            if (strongest.isAssignableFrom(cls)) {
                strongest = cls;
            } else if (!cls.isAssignableFrom(strongest)) {
                return Object.class;
            }
        }
        // Will be Object.class if there were no shared interfaces (or shared interfaces were not
        // compatible).
        return strongest;
    }

    private static Set<Class<?>> getFlattenedInterfaces(Class cls) {
        final Set<Class<?>> set = new HashSet<>();
        while (cls != null && cls != Object.class) {
            for (Class iface : cls.getInterfaces()) {
                collectInterfaces(set, iface);
            }
            cls = cls.getSuperclass();
        }
        return set;
    }

    private static void collectInterfaces(final Collection<Class<?>> into, final Class<?> cls) {
        if (into.add(cls)) {
            for (final Class<?> iface : cls.getInterfaces()) {
                if (into.add(iface)) {
                    collectInterfaces(into, iface);
                }
            }
        }
    }


    public static Class classForName(String className) throws ClassNotFoundException {
        Class result = primitiveClassNameToClass.get(className);
        if (result == null) {
            return Class.forName(className);
        } else {
            return result;
        }

    }

    public static abstract class TypeBoxer<T> {
        public abstract T get(T result);
    }

    public static TypeBoxer getTypeBoxer(Class type) {
        if (type == byte.class || type == Byte.class) {
            return new TypeBoxer<Byte>() {
                @Override
                public Byte get(Byte result) {
                    return (result == NULL_BYTE ? null : result);
                }
            };
        } else if (type == char.class || type == Character.class) {
            return new TypeBoxer<Character>() {
                @Override
                public Character get(Character result) {
                    return (result == NULL_CHAR ? null : result);
                }
            };
        } else if (type == double.class || type == Double.class) {
            return new TypeBoxer<Double>() {
                @Override
                public Double get(Double result) {
                    return (result == NULL_DOUBLE ? null : result);
                }
            };
        } else if (type == float.class || type == Float.class) {
            return new TypeBoxer<Float>() {
                @Override
                public Float get(Float result) {
                    return (result == NULL_FLOAT ? null : result);
                }
            };
        } else if (type == int.class || type == Integer.class) {
            return new TypeBoxer<Integer>() {
                @Override
                public Integer get(Integer result) {
                    return (result == NULL_INT ? null : result);
                }
            };
        } else if (type == long.class || type == Long.class) {
            return new TypeBoxer<Long>() {
                @Override
                public Long get(Long result) {
                    return (result == NULL_LONG ? null : result);
                }
            };
        } else if (type == short.class || type == Short.class) {
            return new TypeBoxer<Short>() {
                @Override
                public Short get(Short result) {
                    return (result == NULL_SHORT ? null : result);
                }
            };
        } else {
            return new TypeBoxer() {
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
