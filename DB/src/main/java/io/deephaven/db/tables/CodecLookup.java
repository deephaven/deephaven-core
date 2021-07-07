package io.deephaven.db.tables;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.ColumnToCodecMappings;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ExternalizableCodec;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SerializableCodec;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.Externalizable;

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
     * @param dataType      The data type to check
     * @param componentType The component type to check, for array and {@link io.deephaven.db.tables.dbarrays.DbArrayBase} types
     * @return Whether a codec is required
     */
    public static boolean codecRequired(@NotNull final Class<?> dataType, @Nullable final Class<?> componentType) {
        if (dataType.isPrimitive() || dataType == Boolean.class || dataType == DBDateTime.class || dataType == String.class || StringSet.class.isAssignableFrom(dataType)) {
            // Primitive, basic, and special types do not require codecs
            return false;
        }
        if (dataType.isArray()) {
            if (componentType == null || !dataType.getComponentType().isAssignableFrom(componentType)) {
                throw new IllegalArgumentException("Array type " + dataType + " does not match component type " + componentType);
            }
            // Arrays of primitives or basic types do not require codecs
            return !(componentType.isPrimitive() || componentType == Boolean.class || componentType == DBDateTime.class || componentType == String.class);
        }
        if (DbArrayBase.class.isAssignableFrom(dataType)) {
            if (componentType == null) {
                throw new IllegalArgumentException("Vector type " + dataType + " requires a component type");
            }
            if (DbArray.class.isAssignableFrom(dataType)) {
                // DbArrays of basic types do not require codecs
                return !(componentType == Boolean.class || componentType == DBDateTime.class || componentType == String.class);
            }
            // DbArrayBases of primitive types do not require codecs
            return false;
        }
        // Anything else must have a codec
        return true;
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
            @NotNull final ColumnToCodecMappings codecMappings
    ) {
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
     * Lookup an {@link ObjectCodec} for the supplied data type, codec class name, and arguments.
     * Assumes that the data type is appropriate for use with a codec, i.e. that {@link #codecRequired(Class, Class)}
     * will return false.
     *
     * @param dataType       The data type
     * @param codecClassName The codec class name
     * @param codecArguments The codec arguments in string form
     * @return The {@link ObjectCodec}
     */
    public static <TYPE> ObjectCodec<TYPE> lookup(@NotNull final Class<TYPE> dataType, final String codecClassName, final String codecArguments) {
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
