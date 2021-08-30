package io.deephaven.util.codec;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Cache for {@link ObjectCodec} instances.
 */
public enum CodecCache {

    DEFAULT;

    /**
     * Cached item.
     */
    private static class Item {

        @NotNull
        private final ObjectCodec<?> codec;

        private Item(@NotNull final String className,
            @Nullable final String arguments) {
            Require.neqNull(className, "className");

            final Class<? extends ObjectCodec> codecClass;
            try {
                // noinspection unchecked
                codecClass = (Class<? extends ObjectCodec>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new CodecCacheException("Unknown codec class " + className, e);
            }
            final Constructor<? extends ObjectCodec> codecConstructor;
            try {
                codecConstructor = codecClass.getConstructor(String.class);
            } catch (NoSuchMethodException e) {
                throw new CodecCacheException(
                    "Codec class " + codecClass + " is missing expected String constructor", e);
            }
            try {
                codec = codecConstructor.newInstance(arguments);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | ClassCastException e) {
                throw new CodecCacheException("Failed to instantiate codec of type " + codecClass
                    + " from arguments " + arguments, e);
            }
        }
    }

    private final Map<String, Map<String, Item>> items = new HashMap<>();

    /**
     * Get or create a cached {@link ObjectCodec}.
     *
     * @param className The class name
     * @param arguments The constructor arguments
     * @param <TYPE> The encoding type
     * @return The corresponding {@link ObjectCodec}
     * @throws CodecCacheException If an error occurred while instantiating the named codec class
     *         from the supplied arguments
     */
    public synchronized <TYPE> ObjectCodec<TYPE> getCodec(@NotNull final String className,
        @Nullable final String arguments) {
        // noinspection unchecked
        return (ObjectCodec<TYPE>) items
            .computeIfAbsent(className, (cn) -> new HashMap<>())
            .computeIfAbsent(arguments, (a) -> new Item(className, a)).codec;
    }

    /**
     * Get the default {@link ObjectCodec} class to use for the given column type.
     *
     * @param dataType The column data type
     * @return The name of the default {@link ObjectCodec} subclass to use for encoding the given
     *         type
     */
    public static String getDefaultCodecClass(@NotNull final Class<?> dataType) {
        if (dataType.equals(LocalDate.class)) {
            return LocalDateCodec.class.getName();
        } else if (dataType.equals(LocalTime.class)) {
            return LocalTimeCodec.class.getName();
        } else if (dataType.equals(BigDecimal.class)) {
            return BigDecimalCodec.class.getName();
        } else if (dataType.equals(BigInteger.class)) {
            return BigIntegerCodec.class.getName();
        } else {
            return null;
        }
    }
}
