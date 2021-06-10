package io.deephaven.db.tables;

import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ExternalizableCodec;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SerializableCodec;

import javax.validation.constraints.NotNull;
import java.io.Externalizable;

/**
 * Utility class to concentrate {@link ObjectCodec} lookups.
 */
public class CodecLookup {

    /**
     * Test whether an explicit codec has been set.
     *
     * @param columnDefinition The codec class name
     * @return Whether an explicit codec has been set
     */
    public static boolean explicitCodecPresent(@NotNull final ColumnDefinition<?> columnDefinition) {
        return explicitCodecPresent(columnDefinition.getObjectCodecClass());
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
     * appropriate for use with a codec, i.e. that it is neither primitive nor one of the object types with special
     * handling.
     *
     * @param columnDefinition The {@link ColumnDefinition}
     * @return The {@link ObjectCodec}
     */
    public static <TYPE> ObjectCodec<TYPE> lookup(@NotNull final ColumnDefinition<TYPE> columnDefinition) {
        final ObjectCodec<TYPE> codec = lookup(columnDefinition.getDataType(), columnDefinition.getObjectCodecClass(), columnDefinition.getObjectCodecArguments());
        if (codec == null) {
            throw new UnsupportedOperationException("Failed to find a matching codec for " + columnDefinition);
        }
        if (columnDefinition.isFixedWidthObjectType() && codec.expectedObjectWidth() != columnDefinition.getObjectWidth()) {
            throw new UnsupportedOperationException("Fixed-width codec mismatch for " + columnDefinition + ", expected width " + codec.expectedObjectWidth());
        }
        return codec;
    }

    /**
     * Lookup an {@link ObjectCodec} for the supplied data type, codec class name, and arguments.
     * Assumes that the data type is appropriate for use with a codec, i.e. that it is neither primitive nor one with
     * special handling.
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
