package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.Vector;
import io.deephaven.stringset.StringSet;
import io.deephaven.time.DateTime;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ExternalizableCodec;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SerializableCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.math.BigDecimal;

/**
 * Utility class to concentrate {@link ObjectCodec} lookups.
 */
public class CodecLookup {

    /**
     * Test whether a codec is required to write or read the supplied {@link ColumnDefinition}.
     *
     * @param columnDefinition The {@link ColumnDefinition}
     * @return Whether a codec is required
     */
    public static boolean codecRequired(@NotNull final ColumnDefinition<?> columnDefinition) {
        return codecRequired(columnDefinition.getDataType(), columnDefinition.getComponentType());
    }

    /**
     * Test whether a codec is required to write or read the supplied types.
     *
     * @param dataType The data type to check
     * @param componentType The component type to check, for array and {@link Vector} types
     * @return Whether a codec is required
     */
    public static boolean codecRequired(@NotNull final Class<?> dataType, @Nullable final Class<?> componentType) {
        if (dataType.isPrimitive() || noCodecRequired(dataType) || StringSet.class.isAssignableFrom(dataType)) {
            // Primitive, basic, and special types do not require codecs
            return false;
        }
        if (dataType.isArray()) {
            if (componentType == null || !dataType.getComponentType().isAssignableFrom(componentType)) {
                throw new IllegalArgumentException(
                        "Array type " + dataType + " does not match component type " + componentType);
            }
            // Arrays of primitives or basic types do not require codecs
            return !(componentType.isPrimitive() || noCodecRequired(dataType));
        }
        if (Vector.class.isAssignableFrom(dataType)) {
            if (componentType == null) {
                throw new IllegalArgumentException("Vector type " + dataType + " requires a component type");
            }
            if (ObjectVector.class.isAssignableFrom(dataType)) {
                // Vectors of basic types do not require codecs
                return !noCodecRequired(dataType);
            }
            // VectorBases of primitive types do not require codecs
            return false;
        }
        // Anything else must have a codec
        return true;
    }

    private static boolean noCodecRequired(@NotNull final Class<?> dataType) {
        return dataType == Boolean.class ||
                dataType == DateTime.class ||
                dataType == String.class ||
                // A BigDecimal column maps to a logical type of decimal, with
                // appropriate precision and scale calculated from column data,
                // unless the user explicitly requested something else
                // via instructions.
                dataType == BigDecimal.class;
    }

    /**
     * Test whether an explicit codec has been set.
     *
     * @param codecClassName The codec class name
     * @return Whether an explicit codec has been set
     */
    public static boolean explicitCodecPresent(final String codecClassName) {
        return codecClassName != null && !codecClassName.isEmpty();
    }

    /**
     * Lookup an {@link ObjectCodec} for the supplied {@link ColumnDefinition}. Assumes that the data type is
     * appropriate for use with a codec, i.e. that {@link #codecRequired(Class, Class)} will return false.
     *
     * @param columnDefinition The {@link ColumnDefinition}
     * @return The {@link ObjectCodec}
     */
    public static <TYPE> ObjectCodec<TYPE> lookup(
            @NotNull final ColumnDefinition<TYPE> columnDefinition,
            @NotNull final ColumnToCodecMappings codecMappings) {
        final String colName = columnDefinition.getName();
        final ObjectCodec<TYPE> codec = lookup(
                columnDefinition.getDataType(),
                codecMappings.getCodecName(colName),
                codecMappings.getCodecArgs(colName));
        if (codec == null) {
            throw new UnsupportedOperationException("Failed to find a matching codec for " + columnDefinition);
        }
        return codec;
    }

    /**
     * Lookup an {@link ObjectCodec} for the supplied data type, codec class name, and arguments. Assumes that the data
     * type is appropriate for use with a codec, i.e. that {@link #codecRequired(Class, Class)} will return false.
     *
     * @param dataType The data type
     * @param codecClassName The codec class name
     * @param codecArguments The codec arguments in string form
     * @return The {@link ObjectCodec}
     */
    public static <TYPE> ObjectCodec<TYPE> lookup(@NotNull final Class<TYPE> dataType, final String codecClassName,
            final String codecArguments) {
        if (explicitCodecPresent(codecClassName)) {
            return CodecCache.DEFAULT.getCodec(codecClassName, codecArguments);
        }
        return getDefaultCodec(dataType);
    }

    /**
     * Get the default codec for the supplied data type.
     *
     * @param dataType The data type
     * @return The default {@link ObjectCodec}
     */
    public static <TYPE> ObjectCodec<TYPE> getDefaultCodec(@NotNull final Class<TYPE> dataType) {
        if (Externalizable.class.isAssignableFrom(dataType)) {
            return CodecCache.DEFAULT.getCodec(ExternalizableCodec.class.getName(), dataType.getName());
        }
        return SerializableCodec.create();
    }
}
