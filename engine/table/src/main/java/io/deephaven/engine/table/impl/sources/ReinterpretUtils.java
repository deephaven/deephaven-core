package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

public class ReinterpretUtils {

    /**
     * Given a DateTime column source turn it into a long column source, either via reinterpretation or wrapping.
     *
     * @param source the source to turn into a long source
     *
     * @return the long source
     */
    public static ColumnSource<Long> dateTimeToLongSource(ColumnSource<?> source) {
        if (source.allowsReinterpret(long.class)) {
            return source.reinterpret(long.class);
        } else {
            // noinspection unchecked
            return new DateTimeAsLongColumnSource((ColumnSource<DateTime>) source);
        }
    }

    /**
     * Given a Boolean column source turn it into a byte column source, either via reinterpretation or wrapping.
     *
     * @param source the source to turn into a byte source
     *
     * @return the byte source
     */
    public static ColumnSource<Byte> booleanToByteSource(ColumnSource<?> source) {
        if (source.allowsReinterpret(byte.class)) {
            return source.reinterpret(byte.class);
        } else {
            // noinspection unchecked
            return new BooleanAsByteColumnSource((ColumnSource<Boolean>) source);
        }
    }

    /**
     * If source is something that we prefer to handle as a primitive, do the appropriate conversion.
     *
     * @param source The source to convert
     * @return If possible, the source converted to a primitive, otherwise the source
     */
    public static ColumnSource<?> maybeConvertToPrimitive(ColumnSource<?> source) {
        if (source.getType() == Boolean.class || source.getType() == boolean.class) {
            return booleanToByteSource(source);
        }
        if (source.getType() == DateTime.class) {
            return dateTimeToLongSource(source);
        }
        return source;
    }

    /**
     * Reinterpret or box {@link ColumnSource} back to its original type.
     *
     * @param originalType The type to convert to
     * @param source The source to convert
     * @return Reinterpret or box source back to the original type if possible
     */
    public static ColumnSource<?> convertToOriginal(@NotNull final Class<?> originalType,
            @NotNull final ColumnSource<?> source) {
        if (originalType == Boolean.class) {
            if (source.getType() != byte.class) {
                throw new UnsupportedOperationException(
                        "Cannot convert column of type " + source.getType() + " to Boolean");
            }
            // noinspection unchecked
            return source.allowsReinterpret(Boolean.class) ? source.reinterpret(Boolean.class)
                    : new BoxedColumnSource.OfBoolean((ColumnSource<Byte>) source);
        }
        if (originalType == DateTime.class) {
            if (source.getType() != long.class) {
                throw new UnsupportedOperationException(
                        "Cannot convert column of type " + source.getType() + " to DateTime");
            }
            // noinspection unchecked
            return source.allowsReinterpret(DateTime.class) ? source.reinterpret(DateTime.class)
                    : new BoxedColumnSource.OfDateTime((ColumnSource<Long>) source);
        }
        throw new UnsupportedOperationException("Unsupported original type " + originalType);
    }
}
